## Some Notes

Might need to change the database to store the graph - requiring export in addition to import (bulk insert).

- [x] But first need to have the graph to be built based on the same data type;
- [x] Simplify things. Clean dead code and separate concerns.
- [x] Then I need to expand current data type to maintain a record of timestamps with last change;
- [x] Then I need to migrate this to work with a permanent storage - either relational or graph database.
  - Currently using the original relational database for simplicity - it was already working.
- [ ] Make use of the known files while processing files to reduce the time.

Continuous Improvement
- [x] need to separate path definition from file processing.
- [ ] handle Inspecting directory related panics
- [ ] restructure code to allow proper integration tests
  - main has minimal logic, everything else lives inside the lib crate.
  

Graph database would be preferable since it would be new knowledge.
