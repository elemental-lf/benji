---
name: Bug report
about: Create a report to help improve Benji
title: ''
labels: bug
assignees: ''

---

**Describe the bug**

A clear and concise description of what the bug is.

**To Reproduce**

Steps to reproduce the behavior. 

Please always  include the following information in this section:
 - your Benji configuration
 - the complete commands with arguments needed to reproduce the behavior
- If a command is failing or showing incorrect behavior run it with `--log-level DEBUG` and include the output here. The `--log-level` option needs to be specified directly after the `benji` command, e.g. `benji --log-level DEBUG backup ...`. If the output is too large provide a public download link instead.

**Remember to redact any credentials and sensitive information!**

**Expected behavior**

A clear and concise description of what you expected to happen.

**Platform and versions (please complete the following information):**

 - Linux distribution name and version
 - Benji version (output of `benji version-info` or git commit digest/branch if applicable)
 - the output of `python -V` from the same (virtual) Python environment that Benji runs in
 - the output of `pip list` from the same (virtual) Python environment that Benji runs in

**Additional context**

Add any other context about the problem here.
