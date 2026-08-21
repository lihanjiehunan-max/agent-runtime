export class DomainError extends Error {
  constructor(code, message, status = 400, details = undefined) {
    super(message);
    this.name = 'DomainError';
    this.code = code;
    this.status = status;
    this.details = details;
  }
}

export function invariant(condition, code, message, status = 400, details = undefined) {
  if (!condition) throw new DomainError(code, message, status, details);
}
