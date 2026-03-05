import json
from service.sync_mod import domain
p='produtos/ProdutosOrigem.json'
with open(p,'r',encoding='utf-8') as f:
    data=json.load(f)
prod=data[0]
infos=domain._get_infos_from_product(prod)
vars=domain._get_variacoes_from_product(prod)
print('infos_len=',len(infos))
print('infos_names=',[i.get('nome') for i in infos])
print('vars_len=',len(vars))
print('first_var_keys=', list(vars[0].keys()) if vars else None)
