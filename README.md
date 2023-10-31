# sqlgraphql
Library for generating SQL queries from GraphQL queries using graphene.

## Planned features
- Query definition with implicit columns (e.g. `select(UserDB)`)
- Transformation of all common sqlalchemy types to GQL type
  - Enum types
  - Ability to add your own transformations
- Handling of primary keys (transformation into ID)
  - Composite primary keys?
- Filtering logic
  - Input object creation with oneof
  - Composite filters with and/or/not
- Sorting logic
- Efficient queries
  - Defining relations (1..n, n..1, n..n?)
  - Walking AST to create single query for (n..1 relation)
  - Batching or other queries (n..n relations)
  - Walking AST breath first instead of depth first?
