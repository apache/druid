---
name: Proposal
about: A template for major Druid change proposals
title: ""
labels: Proposal, Design Review
assignees: ''

---

### Motivation

A description of the problem.

### Proposed changes

This section should provide a detailed description of the changes being proposed. This will usually be the longest section; please feel free to split this section or other sections into subsections if needed.

This section should include any changes made to user-facing interfaces, for example:
- Parameters
- JSON query/ingest specs
- SQL language
- Emitted metrics

### Rationale

A discussion of why this particular solution is the best one. One good way to approach this is to discuss other alternative solutions that you considered and decided against. This should also include a discussion of any specific benefits or drawbacks you are aware of.

### Operational impact

This section should describe how the proposed changes will impact the operation of existing clusters. It should answer questions such as:

- Is anything going to be deprecated or removed by this change? How will we phase out old behavior?
- Is there a migration path that cluster operators need to be aware of?
- Will there be any effect on the ability to do a rolling upgrade, or to do a rolling _downgrade_ if an operator wants to switch back to a previous version?

### Test plan (optional)

An optional discussion of how the proposed changes will be tested. This section should focus on higher level system test strategy and not unit tests (as UTs will be implementation dependent). 

### Future work (optional)

An optional discussion of things that you believe are out of scope for the particular proposal but would be nice follow-ups. It helps show where a particular change could be leading us. There isn't any commitment that the proposal author will actually work on the items discussed in this section.
