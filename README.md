# P2P - Chave-Valor
Trabalho prático 3 da disciplina Redes de Computadores, UFMG, 2019/1

Servent (node) lê um banco de chaves e valores que podem ser acessados por seus clientes.

Cada servent pode possuir outros servents como vizinhos, e múltiplos clientes.

## Execução

**Servent:**
```
./TP3node.py <PORTO> <ARQUIVO_BANCO> [<IP1:PORTO1> … <IPX:PORTOX>]
```

**Cliente:**
```
./TP3client.py <PORTO_C> <IP:PORTO_S>
```


