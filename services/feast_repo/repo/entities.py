from feast import Entity

user = Entity(
    name="user",
    join_keys=["user_id"],
    description="Utilisateur de la plateforme StreamFlow, identifié de manière unique par user_id",
)