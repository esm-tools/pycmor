"""Marker for a rule that has nothing to write on this run."""


class RuleSkipped:
    """Pipeline payload meaning "this rule ends here, without output".

    A step returns this instead of data when the rule legitimately has
    nothing to do, for example a decadal rule in a year that does not close
    a decade. The pipeline runners stop at it and ``_process_rule`` counts
    the rule as done. It is a return value rather than an exception so it
    passes through Prefect tasks like any other payload.
    """

    def __init__(self, reason: str):
        self.reason = reason

    def __repr__(self):
        return f"RuleSkipped({self.reason!r})"
