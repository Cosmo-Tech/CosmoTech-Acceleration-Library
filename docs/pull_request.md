---
description: "Simple check list to follow before making a pull request"
---

Before submitting your pull request, make sure you've completed all the necessary steps:

1. Code Quality
    - [ ] Code follows the project's style guidelines (Black formatting)
    - [ ] All linting checks pass
    - [ ] Code is well-documented with docstrings
    - [ ] Code is efficient and follows best practices
    - [ ] No unnecessary dependencies are added
2. Testing
    - [ ] All unit tests pass
    - [ ] Test coverage meets or exceeds 80%
    - [ ] All functions have at least one test
    - [ ] Edge cases and error conditions are tested
    - [ ] Mocks are used for external services
3.  Documentation
    - [ ] API documentation is updated
    - [ ] Command help text is clear and comprehensive
    - [ ] Translation strings are added for all user-facing text
    - [ ] Usage examples are provided
    - [ ] Any necessary tutorials are created or updated
4. Integration
    - [ ] New functionality integrates well with existing code
    - [ ] No breaking changes to existing APIs
    - [ ] Dependencies are properly specified in pyproject.toml
    - [ ] Command is registered in the appropriate __init__.py file
5. Pull Request Description
    - [ ] Clear description of the changes
    - [ ] Explanation of why the changes are needed
    - [ ] Any potential issues or limitations
    - [ ] References to related issues or discussions
