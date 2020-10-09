## Some Notes

Might need to change the database to store the graph - requiring export in addition to import (bulk insert).

- [x] But first need to have the graph to be built based on the same data type;
- [x] Simplify things. Clean dead code and separate concerns.
- [ ] Then I need to expand current data type to maintain a record of timestamps with last change;
- [ ] Then I need to migrate this to work with a permanent storage - either relational or graph database.

Continuous Improvement
- [x] need to separate path definition from file processing.
- [ ] handle Inspecting directory related panics
- [ ] restructure code to allow proper integration tests
  - main has minimal logic, everything else lives inside the lib crate.
  

Graph database would be preferable since it would be new knowledge.
