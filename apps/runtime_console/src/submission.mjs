/** Page-memory submission recovery; never retries an Execution automatically. */
export class PendingSubmissions {
  constructor(makeKey = () => crypto.randomUUID()) {
    this.makeKey = makeKey;
    this.entries = new Map();
  }

  pending(session) {
    const entry = this.entries.get(session);
    return entry ? {session, message: entry.message, key: entry.key} : null;
  }

  clear() { this.entries.clear(); }

  submit(session, message, post) {
    let entry = this.entries.get(session);
    if (entry && entry.message !== message) {
      return Promise.reject(new Error('上一次提交结果尚未确认，请先使用原消息重试；不要改写为新任务。'));
    }
    if (entry?.promise) return entry.promise;
    if (!entry) {
      entry = {message, key: this.makeKey(), promise: null};
      this.entries.set(session, entry);
    }
    const attempt = entry;
    const forget = () => {
      // A response from before logout must not affect a later connection.
      if (this.entries.get(session) === attempt) this.entries.delete(session);
    };
    attempt.promise = Promise.resolve()
      .then(() => post({message: attempt.message, request_key: attempt.key}))
      .then(receipt => {
        if (!receipt || typeof receipt.execution_id !== 'string' || !receipt.execution_id) {
          throw new Error('提交回执不完整，请使用原消息重试确认。');
        }
        forget();
        return receipt;
      })
      .catch(error => {
        // Network loss, truncated JSON, timeouts and server errors are ambiguous.
        // Only a definitive client rejection can release an unacknowledged key.
        const status = error?.status;
        if (status >= 400 && status < 500 && ![408, 425, 429].includes(status)) forget();
        throw error;
      })
      .finally(() => { attempt.promise = null; });
    return attempt.promise;
  }
}
