#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
UFMG - ICEx - DCC - Redes de Computadores - 2019/1
Aluna: Scarlet Gianasi Viana
Matrícula: 2016006891

TP3 - PEER-TO-PEER CHAVE-VALOR
SERVENT

Versão utilizada: Python 3.6.7

'''

import sys, struct, socket, select, queue, os

# Definições de tipos de mensagens
IDmsg = 4
KEYREQ = 5
TOPOREQ = 6
KEYFLOOD = 7
TOPOFLOOD = 8
RESP = 9

def leBanco(arquivo):
    chaveValor = {}
    
    with open(arquivo, "r" ) as banco:
        for linha in banco:
            if linha[0] != '#':
                print(linha)
                chaveValor[linha.split(None, 1)[0]] = linha.split(" ", 1)[1]
            
    return chaveValor

# Servents podem enviar mensagens de flood, resp e id
def criaIDmsg():    
    '''
    ID
    
    +--2--+-----------2-----------+
    |  4  |  PORTO (0 se servent) |
    +-----+-----------------------+
    
    '''    
    tipo = IDmsg
    porto = porta    
    idmsg = struct.pack("!hh", tipo, porto)    
    return idmsg

def criaFloodMsg(t, info, nseq, portoC, ttl):    
    '''
    KEYFLOOD OU TOPOFLOOD
    
    +----------2----------+--2--+---4--+----4----+------2-----+----2----+--//--+
    | 7 (KEY) ou 8 (TOPO) | TTL | NSEQ | IP_ORIG | PORTO_ORIG | TAMANHO | INFO |
    +---------------------+-----+------+---------+------------+---------+--//--+
    '''    
    tipo = t
    #TTL = 3
    enderecoIP = '127.0.0.1'
    IP = enderecoIP.split(".")
    #porto = porta
    valor = bytes(info, 'ascii')
    tamanho = len(valor)    
    floodmsg = struct.pack("!hhibbbbhh%ds"%(tamanho,), tipo, ttl, nseq, int(IP[0]),int(IP[1]),int(IP[2]),int(IP[3]),portoC,tamanho,valor)    
    return floodmsg

def criaRespMsg(valor, nseq):    
    '''
    RESP
    
    +--2--+---4--+----2----+---//--+
    |  9  | NSEQ | TAMANHO | VALOR |
    +-----+------+---------+---//--+
    
    '''    
    tipo = RESP
    v = bytes(valor, 'ascii')
    tamanho = len(v)    
    resp = struct.pack("!hih%ds"%(tamanho,), tipo, nseq, tamanho, v)    
    return resp

def leResp(s):
    # Lê nseq
    msg = s.recv(4)
    msgUnp = struct.unpack("!i", msg)
    nseqRecv = msgUnp[0]    
    # Lê tamanho
    msg = s.recv(2)
    msgUnp = struct.unpack("!h", msg)
    tamanho = msgUnp[0]    
    # Lê valor, baseado no tamanho
    msg = s.recv(tamanho)
    msgUnp = struct.unpack("!%ds"%(tamanho), msg)
    valor = str(msgUnp[0], 'ascii')    
    print("Resp - valor: "+valor)  
        
def leFlood(s):
    msg = s.recv(2)
    msgUnp = struct.unpack("!h", msg)
    TTL = msgUnp[0]    
    msg = s.recv(4)
    msgUnp = struct.unpack("!i", msg)
    nseqRecv = msgUnp[0]    
    IP_ORIG = ""
    for i in range(0,4):
        msg = s.recv(1)
        msgUnp = struct.unpack("!b", msg)
        IP_ORIG += str(msgUnp[0])
        if i < 3:
            IP_ORIG += '.'            
    msg = s.recv(2)
    msgUnp = struct.unpack("!h", msg)
    PORTO_ORIG = msgUnp[0]               
    msg = s.recv(2)
    msgUnp = struct.unpack("!h", msg)
    tamanho = msgUnp[0]    
    msg = s.recv(tamanho)
    msgUnp = struct.unpack("!%ds"%(tamanho), msg)
    info = str(msgUnp[0], 'ascii')
    
    print("Flood - TTL: "+str(TTL)+", nseq: "+str(nseqRecv)+", IP: "+IP_ORIG+", PORTO: "+str(PORTO_ORIG)+", INFO: "+info)
    
    return TTL, nseqRecv, IP_ORIG, PORTO_ORIG, info
        
def leTopoReq(s):
    msg = s.recv(4)
    msgUnp = struct.unpack("!i", msg)
    nseqRecv = msgUnp[0]
    print("TopoReq - nseq: "+str(nseqRecv))
    return nseqRecv

def leKeyReq(s):
    msg = s.recv(4)
    msgUnp = struct.unpack("!i", msg)
    nseqRecv = msgUnp[0]    
    msg = s.recv(2)
    msgUnp = struct.unpack("!h", msg)
    tamanho = msgUnp[0]    
    msg = s.recv(tamanho)
    msgUnp = struct.unpack("!%ds"%(tamanho), msg)
    chave = str(msgUnp[0], 'ascii')
    
    print("KeyReq - nseq: "+str(nseqRecv)+", chave: "+chave)
    
    return nseqRecv, chave
        
def leIDmsg(s):
    msg = s.recv(2)
    msgUnp = struct.unpack("!h", msg)
    porto = msgUnp[0]    
    if porto == 0:
        print("IDmsg de servent")
        return 'servent', 0
    else:
        print("IDmsg - porto: "+str(porto))
        return 'client', porto
    
def alagamento(msg, s, conn):
    print("Iniciando alagamento.")
    for i in conn:
        if i is not server:
            if i is not s:
                print("Enviando mensagem de alagamento.")
                i.send(msg)  
                
def conectaCliente(msg, ipC, portoC):
    c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    end = (ipC,portoC)
    print("Conectando ao cliente "+str(ipC)+":"+str(portoC))
    c.bind(end)
    c.connect(end)
    c.setblocking(0)
    #inputs.append(c)
    c.send(msg)
    c.close()    
    #message_queues[c] = queue.Queue()
    #message_queues[c].put(msg)

def recebeMsg(s, tipoRecv, i):
    # Espera receber ID, requisiçao e floods    
    # Mensagem de identificação
    if tipoRecv == IDmsg:
        print("Mensagem ID")
        origem, p = leIDmsg(s)   
        if origem == 'servent':
            # Faz algo
            pass
        elif origem == 'client':
            # Faz algo
            pass
        # Não ha mensagem para enviar
    # Mensagem de requisição de chave    
    elif tipoRecv == KEYREQ:
        nseqRecv, chave = leKeyReq(s)    
        # Procura a chave em seu banco
        if chave in banco:
            print("Eu tenho a chave!")
            # Manda resp com a chave pro cliente
            resposta = criaRespMsg(banco[chave], nseqRecv)
            return resposta            
        else:
            print("Não tenho a chave, repassando requisição...")
            print("PORTO ORIGEM:"+str(s.getsockname()[1]))
            msg = criaFloodMsg(KEYFLOOD, chave, nseqRecv, portoC, 3) 
            # Envia mensagem para todos seus vizinhos
            # Exceto o que enviou a mensagem
            alagamento(msg, s, i)            
    
    # Mensagem de requisição de topologia      
    elif tipoRecv == TOPOREQ:
        nseqRecv = leTopoReq(s)   
        # Envia topologia... comeca com o endereco do proprio servent
        topologia = '127.0.0.1:'+str(porta)+' '
        
        msg = criaFloodMsg(TOPOFLOOD, topologia, nseqRecv, portoC, 3)        
        alagamento(msg, s, i)
        print("PORTO ORIGEM:"+str(s.getsockname()[1]))
        
        resposta = criaRespMsg(topologia, nseqRecv)
        return resposta
        # Enviara para os outros servents
        # Envia topologia pro cliente de volta também
        
    elif tipoRecv == KEYFLOOD or tipoRecv == TOPOFLOOD:                
        ttl, nseqRecv, ip_orig, porto_orig, info = leFlood(s)  
        # Atualiza Time-to-Live
        ttl -= 1
        
        # Checa se amensagem não é repetida
        # Se não for, adiciona na tabela
        if porto_orig in mensagens:
            if nseqRecv in mensagens[porto_orig]:
                print("Mensagem de alagamento repetida.")
                # Se for repetida, não retransmite
                return False
        else:
            if porto_orig in mensagens:
                mensagens[porto_orig].append(nseqRecv)
            else: 
                mensagens[porto_orig] = [nseqRecv]
        
        # Se a mensagem ainda está viva
        if ttl > 0:
            # Passa adiante para seus vizinhos
            if tipoRecv == TOPOFLOOD:
                # Adiciona seu próprio endereço a info
                info += '127.0.0.1:'+str(porta)+' '
                # Envia aos vizinhos, e abre conexão com o programa que iniciou
                # a requisição a partir do IP e porto
                # Abre conexão
                # Envia resposta
                msg = criaFloodMsg(TOPOFLOOD, info, nseqRecv, porto_orig, ttl)                
                alagamento(msg, s, i)
                
                resposta = criaRespMsg(info, nseqRecv)
                conectaCliente(resposta, ip_orig, porto_orig)
            else:
                # É KEYFLOOD
                # Procura mensagem no banco
                if info in banco:
                    print("Eu tenho a chave!")
                    # Conecta com o cliente
                    # Envia resp
                    resposta = criaRespMsg(banco[info], nseqRecv)
                    
                    conectaCliente(resposta, ip_orig, porto_orig)
                    
                else:
                    msg = criaFloodMsg(KEYFLOOD, info, nseqRecv, porto_orig, ttl)
                    alagamento(msg, s, i)
        
    elif tipoRecv == RESP:
        # Não importa, essa mensagem nao é esperada pelo servent
        # Mas precisa lê-la pelo bytestream do TCP
        print("Tipo inválido, descartando mensagem.")
        leResp(s)  
    
    return False
        

''' Inicialização do Servent
    Abre a porta e adiciona seus vizinhos '''
porta = int(sys.argv[1])
arquivoBanco = sys.argv[2]
vizinhos = []
for i in range(3, len(sys.argv)):
    vizinhos.append(sys.argv[i])
print("Porto: "+str(porta)+", banco: "+arquivoBanco+"Vizinhos: "+str(vizinhos))

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setblocking(0)
endereco = ('127.0.0.1', porta)
server.bind(endereco)
server.listen(10) # Máx. 10 vizinhos

banco = leBanco(arquivoBanco)
mensagens = {}

inputs = [server]
outputs = []
message_queues = {}
portoC = 9091

# Conecta com seus vizinhos, se existirem
for v in vizinhos:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    h, p = v.split(":")
    print("Cnectando a "+h+", "+p)
    s.connect((h,int(p)))
    s.setblocking(0)
    inputs.append(s)
    # Envia mensagem de ID para o vizinho
    msg = criaIDmsg()
    s.send(msg)
    message_queues[s] = queue.Queue()
    
while inputs:
    try:
        print("Esperando mensagens e conexões...")
        readable, writable, _ = select.select(inputs, outputs, inputs)
        
        for s in readable:
            if s is server:
                conn, add = s.accept()
                print("Nova conexão em "+str(add))
                conn.setblocking(0)
                inputs.append(conn)
                message_queues[conn] = queue.Queue()
            else:
                print("Recebendo mensagem")
                msg = s.recv(2)                     
                if msg:
                    # Recebe o tipo
                    msgUnp = struct.unpack("!h", msg)
                    tipoRecv = msgUnp[0]
                    mensagem = recebeMsg(s, tipoRecv, inputs)
                    if mensagem:
                        message_queues[s].put(mensagem) # Mensagem de resposta na fila
                        print("Teste envio")
                    if s not in outputs:
                        outputs.append(s)
                    # Checar se a mensagem ID veio de cliente ou servidor?
                else:
                    # Desconectou
                    print("Fechando conexão com "+str(s.getpeername()))
                    if s in outputs:
                        outputs.remove(s)
                    inputs.remove(s)
                    s.close()
                    del message_queues[s]                  
        for s in writable:
            try:
                print("Adicionando mensagem na fila")
                next_msg = message_queues[s].get_nowait()
            except queue.Empty:
                outputs.remove(s)
                pass
            else:
                print("Enviando mensagem para "+str(s.getpeername()))
                s.send(next_msg)
    except KeyboardInterrupt:
        print("Finalizando o servidor.")
        for s in inputs:
            s.close()
        server.close()
        os._exit(1)
