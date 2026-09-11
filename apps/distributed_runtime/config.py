"""Environment-only infrastructure and secret configuration."""
from dataclasses import dataclass
import os
from .store import Store
from .artifacts import FileArtifacts, S3Artifacts


@dataclass
class Settings:
    database_url: str
    token: str
    redis_url: str = ''
    artifact_dir: str = './.runtime-artifacts'
    bucket: str = ''
    s3_endpoint: str = ''
    lease_seconds: float = 6
    ops_token: str = ''

    @classmethod
    def from_env(cls, role='api'):
        token = os.environ.get('RUNTIME_API_TOKEN', '')
        if role=='api' and len(token) < 16:
            raise ValueError('RUNTIME_API_TOKEN must contain at least 16 characters')
        ops=os.environ.get('RUNTIME_OPS_TOKEN','')
        if ops and len(ops)<16: raise ValueError('RUNTIME_OPS_TOKEN must contain at least 16 characters')
        if role=='api' and os.environ.get('RUNTIME_PROFILE')=='live' and (not ops or ops==token):
            raise ValueError('Live APIs require separate business and operations credentials')
        return cls(database_url=os.environ['RUNTIME_DATABASE_URL'], token=token,
            redis_url=os.environ.get('RUNTIME_REDIS_URL',''), bucket=os.environ.get('RUNTIME_S3_BUCKET',''),
            s3_endpoint=os.environ.get('RUNTIME_S3_ENDPOINT',''),
            artifact_dir=os.environ.get('RUNTIME_ARTIFACT_DIR','./.runtime-artifacts'),
            lease_seconds=float(os.environ.get('RUNTIME_LEASE_SECONDS','6')),
            ops_token=os.environ.get('RUNTIME_OPS_TOKEN',''))

    def store(self):
        return Store(self.database_url, lease_seconds=self.lease_seconds)

    def artifacts(self):
        return S3Artifacts(self.bucket, endpoint=self.s3_endpoint, initialize=True) if self.bucket else FileArtifacts(self.artifact_dir)
