with open("src/interface_grafica/services/particionamento_fiscal.py", "r") as f:
    content = f.read()

content = content.replace("    norm = lambda s: (remove_accents(s) or \"\").lower().strip()", "    def norm(s):\n        return (remove_accents(s) or \"\").lower().strip()")

with open("src/interface_grafica/services/particionamento_fiscal.py", "w") as f:
    f.write(content)
