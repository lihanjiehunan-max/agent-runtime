"""Immutable, content-addressed artifacts; no mutable shared Agent workspace."""
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from .store import Conflict, canonical

MAX_ARTIFACT_BYTES = 16 * 1024 * 1024


def check_ref(ref: str):
    if not isinstance(ref, str) or not re.fullmatch(r'[a-f0-9]{64}', ref):
        raise Conflict('Invalid artifact reference')


def verify(ref, data):
    if len(data)>MAX_ARTIFACT_BYTES or hashlib.sha256(data).hexdigest()!=ref:
        raise Conflict('Artifact integrity check failed')
    return data


class JsonArtifacts:
    def put_json(self, value) -> str:
        return self.put_bytes(canonical(value))

    def get_json(self, ref: str):
        return json.loads(self.get_bytes(ref))


class FileArtifacts(JsonArtifacts):
    """Local testing, or a shared filesystem mounted identically on all Workers."""
    def __init__(self, root):
        self.root=Path(root).resolve()
        self.root.mkdir(parents=True,exist_ok=True)

    def path(self, ref):
        check_ref(ref)
        return self.root / ref[:2] / ref

    def put_bytes(self,data:bytes) -> str:
        if not isinstance(data,bytes) or len(data)>MAX_ARTIFACT_BYTES:
            raise Conflict('Artifact must contain at most 16 MiB')
        ref=hashlib.sha256(data).hexdigest(); target=self.path(ref)
        target.parent.mkdir(exist_ok=True)
        if target.exists():
            verify(ref,target.read_bytes()); return ref
        fd,tmp=tempfile.mkstemp(dir=target.parent,prefix='.upload-')
        try:
            with os.fdopen(fd,'wb') as f:
                f.write(data); f.flush(); os.fsync(f.fileno())
            os.replace(tmp,target)
        finally:
            if os.path.exists(tmp): os.unlink(tmp)
        return ref

    def get_bytes(self,ref):
        path=self.path(ref)
        if not path.is_file(): raise Conflict('Artifact not found')
        if path.stat().st_size>MAX_ARTIFACT_BYTES: raise Conflict('Artifact too large')
        return verify(ref,path.read_bytes())


class S3Artifacts(JsonArtifacts):
    """S3/MinIO backend. Credentials are resolved only in the server environment."""
    def __init__(self,bucket,*,endpoint=None,initialize=False):
        import boto3
        from botocore.config import Config
        self.bucket=bucket
        self.client=boto3.client('s3',endpoint_url=endpoint,
            config=Config(connect_timeout=3,read_timeout=15,retries={'max_attempts':1},s3={'addressing_style':'path'}))
        if initialize:
            from botocore.exceptions import ClientError
            try: self.client.head_bucket(Bucket=bucket)
            except ClientError as exc:
                if exc.response['Error']['Code'] not in {'404','NoSuchBucket','NotFound'}: raise
                self.client.create_bucket(Bucket=bucket)

    def put_bytes(self,data):
        if not isinstance(data,bytes) or len(data)>MAX_ARTIFACT_BYTES: raise Conflict('Artifact too large')
        ref=hashlib.sha256(data).hexdigest()
        self.client.put_object(Bucket=self.bucket,Key='artifacts/'+ref,Body=data,ContentType='application/json')
        return ref

    def get_bytes(self,ref):
        check_ref(ref)
        body=self.client.get_object(Bucket=self.bucket,Key='artifacts/'+ref)['Body']
        try: data=body.read(MAX_ARTIFACT_BYTES+1)
        finally: body.close()
        return verify(ref,data)
