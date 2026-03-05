from pydantic import BaseModel


class EchoInput(BaseModel):
    message: str


class EchoOutput(BaseModel):
    echoed: str


def echo(input: EchoInput) -> EchoOutput:
    return EchoOutput(echoed=input.message)


async def async_echo(input: EchoInput) -> EchoOutput:
    return EchoOutput(echoed=input.message)


def bad_no_annotations(x):
    return x
