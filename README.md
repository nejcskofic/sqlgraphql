# sqlgraphql
Library for generating SQL queries from GraphQL queries using graphene.

## Done
- Simple select using ORM queries (explicit columns or whole entity) or core queries

## Planned features
- Transformation of all common sqlalchemy types to GQL type
  - ID type
  - Ability to add your own transformations (Enum registry is internal)
- Handling of primary keys (transformation into ID)
  - Composite primary keys?
- Support selection of single entry (by ID)
- Filtering logic
  - Input object creation with oneof
  - Composite filters with and/or/not
- Efficient queries
  - Defining relations (1..n, n..1, n..n?)
  - Walking AST to create single query for (n..1 relation)
  - Batching or other queries (n..n relations)
  - Walking AST breath first instead of depth first?
- Mixed mode definitions (DB query and other pure python side resolvers)
- GQL validation via oneOf directive (custom print_schema + custom validator)
