# sqlgraphql
Library for generating SQL queries from GraphQL queries using graphene.

## Done
- Simple select using ORM queries (explicit columns or whole entity) or core queries
- Mapping of sqlalchemy types to GQL types (including enums and json data)
- Sorting
- Filtering and composite filters (with _and, _or and _not to compose complex filters)

## Planned features
- Transformation of all common sqlalchemy types to GQL type
  - ID type
  - Ability to add your own transformations (Enum registry is internal)
- Handling of primary keys (transformation into ID)
  - Composite primary keys?
- Support selection of single entry (by ID)
- Efficient queries
  - Defining relations (1..n, n..1, n..n?)
  - Walking AST to create single query for (n..1 relation)
  - Batching or other queries (n..n relations)
  - Walking AST breath first instead of depth first?
- Mixed mode definitions (DB query and other pure python side resolvers)
- GQL validation via oneOf directive (custom print_schema + custom validator)
- Multiple root queries/multiple root queries on non root object (verify query transformation work as expected)
- Polymorphic DB models and GQL queries with fragments