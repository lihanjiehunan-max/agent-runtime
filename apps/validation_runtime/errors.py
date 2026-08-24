from pydantic import BaseModel, ConfigDict


class RuntimeErrorDetail(BaseModel):
    model_config = ConfigDict(frozen=True)

    code: str
    message: str


class RuntimeServiceError(Exception):
    def __init__(self, code: str, message: str):
        self.detail = RuntimeErrorDetail(code=code, message=message)
        super().__init__(message)
