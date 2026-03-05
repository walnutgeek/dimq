use pyo3::prelude::*;
use rayon::prelude::*;
use sha2::{Digest, Sha256};
use std::io::Write;
use std::time::{Duration, Instant};

#[pyfunction]
#[pyo3(signature = (duration_seconds=1.0, concurrency=1, cpu_load=0.7, io_load=0.2, memory_mb=50))]
fn run(
    py: Python<'_>,
    duration_seconds: f64,
    concurrency: usize,
    cpu_load: f64,
    io_load: f64,
    memory_mb: usize,
) -> PyResult<PyObject> {
    let result = py.allow_threads(|| {
        let start = Instant::now();
        let mut phases: Vec<(String, f64, f64)> = Vec::new();

        // Memory pressure
        let mem_start = start.elapsed().as_secs_f64();
        let mut _memory: Vec<u8> = vec![0u8; memory_mb * 1024 * 1024];
        // Touch pages to ensure real allocation
        for i in (0.._memory.len()).step_by(4096) {
            _memory[i] = 1;
        }
        phases.push((
            "memory".into(),
            mem_start,
            start.elapsed().as_secs_f64() - mem_start,
        ));

        // CPU load using rayon
        let cpu_duration = Duration::from_secs_f64(duration_seconds * cpu_load);
        let cpu_start = start.elapsed().as_secs_f64();
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(concurrency)
            .build()
            .unwrap();
        pool.install(|| {
            let deadline = Instant::now() + cpu_duration;
            (0..concurrency).into_par_iter().for_each(|_| {
                let mut hasher = Sha256::new();
                let mut counter: u64 = 0;
                while Instant::now() < deadline {
                    hasher.update(counter.to_le_bytes());
                    counter += 1;
                    if counter % 10000 == 0 {
                        let _ = hasher.finalize_reset();
                    }
                }
            });
        });
        phases.push((
            "cpu".into(),
            cpu_start,
            start.elapsed().as_secs_f64() - cpu_start,
        ));

        // IO load
        let io_duration = Duration::from_secs_f64(duration_seconds * io_load);
        let io_start_t = start.elapsed().as_secs_f64();
        let io_deadline = Instant::now() + io_duration;
        while Instant::now() < io_deadline {
            if let Ok(mut tmpfile) = tempfile::tempfile() {
                let data = vec![0u8; 1024 * 64]; // 64KB writes
                let _ = tmpfile.write_all(&data);
                let _ = tmpfile.flush();
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        phases.push((
            "io".into(),
            io_start_t,
            start.elapsed().as_secs_f64() - io_start_t,
        ));

        let total = start.elapsed().as_secs_f64();
        (phases, total, memory_mb)
    });

    // Build Python dict result
    let dict = pyo3::types::PyDict::new_bound(py);
    let phases_list = pyo3::types::PyList::empty_bound(py);
    for (phase_type, start_time, duration) in &result.0 {
        let phase_dict = pyo3::types::PyDict::new_bound(py);
        phase_dict.set_item("type", phase_type)?;
        phase_dict.set_item("start_seconds", start_time)?;
        phase_dict.set_item("duration_seconds", duration)?;
        phases_list.append(phase_dict)?;
    }
    dict.set_item("phases", phases_list)?;
    dict.set_item("total_duration_seconds", result.1)?;
    dict.set_item("peak_memory_mb", result.2)?;

    Ok(dict.into())
}

#[pymodule]
fn load_task(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(run, m)?)?;
    Ok(())
}
