import re

file_path = "/home/stangler/Documents/Python/GER/app_analytics.py"
with open(file_path, "r", encoding="utf-8") as f:
    text = f.read()

# 1. Initialize builder instead of clauses
text = text.replace('clauses = ["1=1"]', 'builder = FiltroAvancadoSpecBuilder()')

# 2. Re-assign curr_where inside the cascading loop to the translated string
text = text.replace('curr_where = " AND ".join(clauses)', 'curr_where = DuckDBCriteriaTranslator.translate(builder.build())')

# 3. Rename "clauses" argument in the builder calls to "builder"
text = re.sub(r',\s*clauses\s*,', ', builder,', text)
text = re.sub(r'clauses\s*,', 'builder,', text) # For arguments like 'clauses, "f_idade"' 

# 4. Handle clauses.append -> builder.add_clausula_legado
text = text.replace('clauses.append(', 'builder.add_clausula_legado(')

# Write back
with open(file_path, "w", encoding="utf-8") as f:
    f.write(text)

print("Transformations applied to app_analytics.py")
