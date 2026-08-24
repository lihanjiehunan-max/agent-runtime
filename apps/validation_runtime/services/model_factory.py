from langchain_openai import ChatOpenAI
from pydantic import SecretStr

from apps.validation_runtime.domain import ModelGatewayProfile


class ModelFactory:
    def create(
        self,
        profile: ModelGatewayProfile,
        key: SecretStr,
    ) -> ChatOpenAI:
        return ChatOpenAI(
            model=profile.model,
            api_key=key,
            base_url=str(profile.base_url).rstrip("/"),
            timeout=profile.read_timeout_seconds,
            max_retries=0,
            use_responses_api=False,
        )
