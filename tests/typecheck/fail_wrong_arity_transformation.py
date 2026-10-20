"""@transformation rejects a function that does not take (records, runtime_params)."""

from reflowfy import Records, transformation


@transformation("typecheck_fail_arity")  # expect: reportArgumentType
def one_arg(records: Records) -> Records:
    return records
