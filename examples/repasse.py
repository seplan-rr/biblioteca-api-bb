from api_bb import AccountabilityV3RepasseAPI

acc = AccountabilityV3RepasseAPI()

# Buscando pelas agências próximas à SEPLAN/RR no Estado de Roraima
df = acc.get_agencias_proximas("84012012000126", "69303475")

print(df)
