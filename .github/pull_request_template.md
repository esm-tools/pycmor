# Thank you for contributing! 🎉

## Summary of the most important changes
Please summarize the most important changes you have made in this PR.

## Usage example in the PR

Please put a small usage example in the PR body so that we can see what you want to do.

You can include python:
```python
from pycmor.core.cmorizer import CMORizer
cmorizer = CMORizer()
cmorizer.process()
```
An example with Rules:
```python
>>> # Assume you have a Rule with these attributes:
>>> rule = Rule()
>>> rule.name = "My Rule"
>>> rule.description = "This is my rule"
>>> # Some input data
>>> data = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
>>> # Using my new step in the PR:
>>> data = my_new_step(data, rule)
>>> data
... # Add what the data should look like
```

These sorts of examples should also be in the docstrings of your code!

## Checklist
+ [ ] I have tested the changes in this PR.
+ [ ] I have updated the documentation.
+ [ ] If I have made a new step which requires new arguments, these are added to the `validate.py` file
      so that the user can see documentation for the arguments.

## Copilot Summary
If you have Github Co-Pilot, please include an automatically generated summary.
