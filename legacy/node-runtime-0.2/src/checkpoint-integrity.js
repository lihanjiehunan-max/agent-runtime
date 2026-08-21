import { createHash } from 'node:crypto';

export const CHECKPOINT_INTEGRITY_VERSION = 2;

function sha256(value) {
  return createHash('sha256').update(value).digest('hex');
}

export function legacyCheckpointHash(checkpoint) {
  return sha256(`${checkpoint.parent_checkpoint_id ?? ''}:${checkpoint.state_json}`);
}

export function checkpointHash(checkpoint) {
  const envelope = {
    format: 'checkpoint-integrity/v2',
    checkpointId: checkpoint.checkpoint_id,
    runId: checkpoint.run_id,
    checkpointSeq: checkpoint.checkpoint_seq,
    parentCheckpointId: checkpoint.parent_checkpoint_id ?? null,
    kind: checkpoint.kind,
    status: checkpoint.status,
    eventSeq: checkpoint.event_seq,
    executionEpoch: checkpoint.execution_epoch,
    schemaVersion: checkpoint.schema_version,
    stateJson: checkpoint.state_json,
    createdAt: checkpoint.created_at,
  };
  return sha256(JSON.stringify(envelope));
}

export function checkpointHashMatches(checkpoint) {
  if (checkpoint.integrity_version === 1) return checkpoint.content_hash === legacyCheckpointHash(checkpoint);
  if (checkpoint.integrity_version === CHECKPOINT_INTEGRITY_VERSION) return checkpoint.content_hash === checkpointHash(checkpoint);
  return false;
}
