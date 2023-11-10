from graphql import DirectiveLocation, GraphQLDirective

# No validation is available for this because there is no nice way to plug into validations
# which are done by graphql-core. Default rules are defined in graphql.specified_rules, but
# this is tuple object. We will do validation on use site in resolver.
GraphQLOneOfDirective = GraphQLDirective(
    name="oneOf",
    locations=[DirectiveLocation.OBJECT, DirectiveLocation.INPUT_OBJECT],
    description=(
        "Marks object of returning exactly one of the fields or input object for accepting"
        " exactly one field argument."
    ),
)
