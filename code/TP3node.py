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
                chaveValor[linha.split(None, 1)[0]] = linha.split(" ", 1)[1]            
    return chaveValor

# Servents podem enviar mensagens de flood, resp e id
def criaIDmsg():     
    tipo = IDmsg
    idmsg = struct.pack("!hh", tipo, 0)    
    return idmsg

def criaFloodMsg(t, info, nseq, portoC, TTL):  
    tipo = t
    enderecoIP = '127.0.0.1'
    IP = enderecoIP.split(".")
    valor = bytes(info, 'ascii')
    tamanho = len(valor)    
    floodmsg = struct.pack("!hhibbbbHh%ds"%(tamanho,), tipo, TTL, nseq, int(IP[0]),int(IP[1]),int(IP[2]),int(IP[3]),portoC,tamanho,valor)    
    return floodmsg

def criaRespMsg(valor, nseq):   
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
    msgUnp = struct.unpack("!H", msg)
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
        if i is not server and i is not s:
        #if i.getpeername() in servents and i is not s:
            print("Enviando alagamento para "+str(i.getpeername()))
            i.send(msg)
        #if i is not server:
        #    if i is not s:
        #        print("Enviando mensagem de alagamento.")
        #        i.send(msg)  
                
def conectaCliente(msg, ipC, portoC):
    print("Conectando com o cliente na porta "+str(portoC))
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((ipC, portoC))
    print("Conectado.")
    client.send(msg)
    client.close()
    
def recebeMsg(s, tipoRecv, i):
    if tipoRecv == IDmsg:
        origem, porto = leIDmsg(s)
        if origem == 'servent':
            print("Mensagem ID recebida de servent")
            servents.append(s.getpeername())
        else:
            print("Mensagem ID recebida de um cliente, porto "+str(s.getpeername()))
            clients.append(s.getpeername())
            ports[s.getpeername()] = porto
    elif tipoRecv == KEYREQ:
        nseqRecv, chave = leKeyReq(s)
        if chave in banco:
            print("Eu tenho a chave.")
            resposta = criaRespMsg(banco[chave], nseqRecv)
            porto = ports[s.getpeername()]
            conectaCliente(resposta, '127.0.0.1', porto)
            return resposta
        else:
            print("Não tenho a chave. Iniciando alagamento de chave.")
            porto_orig = ports[s.getpeername()]
            msg = criaFloodMsg(KEYFLOOD, chave, nseqRecv, porto_orig, 3)
            alagamento(msg, s, i)
    elif tipoRecv == TOPOREQ:
        nseqRecv = leTopoReq(s)
        print("Requisição de topologia recebida. Iniciando alagamento de topologia.")
        topologia = '127.0.0.1:'+str(portaS)
        porto_orig = ports[s.getpeername()]
        msg = criaFloodMsg(TOPOFLOOD, topologia, nseqRecv, porto_orig, 3)
        alagamento(msg, s, i)
        resposta = criaRespMsg(topologia, nseqRecv)
        conectaCliente(resposta, '127.0.0.1', porto_orig)
        return resposta
    elif tipoRecv == KEYFLOOD or tipoRecv == TOPOFLOOD:
        ttl, nseqRecv, ip_orig, porto_orig, info = leFlood(s)
        ttl -= 1
        
        if porto_orig in mensagens:
            if nseqRecv in mensagens[porto_orig]:
                print("Mensagem repetida, descartada.")
                return False
        else:
            if porto_orig in mensagens:
                mensagens[porto_orig].append(nseqRecv)
            else:
                mensagens[porto_orig] = [nseqRecv]
        if ttl > 0:
            if tipoRecv == TOPOFLOOD:
                info += ' 127.0.0.1:'+str(portaS)
                print("Mensagem de alagamento recebida, enviando resposta para "+str(porto_orig))
                msg = criaFloodMsg(TOPOFLOOD, info, nseqRecv, porto_orig, ttl)
                alagamento(msg, s, i)
                resposta = criaRespMsg(info, nseqRecv)
                conectaCliente(resposta, ip_orig, porto_orig)
            else:
                if info in banco:
                    print("Eu tenho a chave.")
                    resposta = criaRespMsg(banco[info], nseqRecv)
                    conectaCliente(resposta, ip_orig, porto_orig)
                else:
                    print("Não possuo a chave, enviando alagamento.")
                    msg = criaFloodMsg(KEYFLOOD, info, nseqRecv, porto_orig, ttl)
                    alagamento(msg, s, i)
    elif tipoRecv == RESP:
        print("Tipo inválido, descartando mensagem.")
        leResp(s)        
    return False                   
    
''' Inicialização do Servent
    Abre a porta e adiciona seus vizinhos '''
portaS = int(sys.argv[1])
arquivoBanco = sys.argv[2]
vizinhos = []
for i in range(3, len(sys.argv)):
    vizinhos.append(sys.argv[i])
print("Porto: "+str(portaS)+", banco: "+arquivoBanco+"Vizinhos: "+str(vizinhos))

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setblocking(0)
endereco = ('127.0.0.1', portaS)
server.bind(endereco)
server.listen(10) # Máx. 10 vizinhos

banco = leBanco(arquivoBanco)
mensagens = {}
clients = []
servents = []
ports = {}
inputs = [server]
outputs = []
message_queues = {}

for v in vizinhos:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    h, p = v.split(":")
    print("Conectando ao vizinho "+h+":"+p)
    s.connect((h,int(p)))
    s.setblocking(0)
    inputs.append(s)
    msg = criaIDmsg()
    s.send(msg)
    message_queues[s] = queue.Queue()
    
while inputs:
    try:
        print("Esperando...")
        readable, writable, _ = select.select(inputs, outputs, inputs)
        for s in readable:
            if s is server:
                conn, addr = s.accept()
                print("Nova conexão com "+str(addr))
                conn.setblocking(0)
                inputs.append(conn)
                message_queues[conn] = queue.Queue()
            else:
                print("Recebendo mensagem.")
                msg = s.recv(2)
                if msg:
                    msgUnp = struct.unpack("!h", msg)
                    tipoRecv = msgUnp[0]
                    mensagem = recebeMsg(s, tipoRecv, inputs)
                    #if mensagem:
                    #    message_queues[s].put(mensagem)
                    if s not in outputs:
                        outputs.append(s)
                else:
                    print("Fechando conexão com "+str(s.getpeername()))
                    if s in outputs:
                        outputs.remove(s)
                    inputs.remove(s)
                    s.close()
                    del message_queues[s]
        for s in writable:
            try:
                print("Adicionando mensagem da fila.")
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
