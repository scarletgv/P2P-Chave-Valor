#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
UFMG - ICEx - DCC - Redes de Computadores - 2019/1
Aluna: Scarlet Gianasi Viana
Matrícula: 2016006891

TP3 - PEER-TO-PEER CHAVE-VALOR
CLIENTE

Versão utilizada: Python 3.6.7

'''
import sys, socket, struct

# Definições de tipos de mensagens
IDmsg = 4
KEYREQ = 5
TOPOREQ = 6
KEYFLOOD = 7
TOPOFLOOD = 8
RESP = 9

# Cliente pode enviar apenas 3 tipos de mensagem:
# Requisição de chave ou req. de topologia
# E mensagem de ID
def criaKeyReq(c, nseq):    
    tipo = KEYREQ
    chave = bytes(c, 'ascii')
    tamanho = len(chave)    
    keyreq = struct.pack("!hih%ds"%(tamanho,), tipo, nseq, tamanho, chave)
    return keyreq

def criaTopoReq(nseq):    
    tipo = TOPOREQ
    toporeq = struct.pack("!hi", tipo, nseq)    
    return toporeq

def criaIDmsg(porta):    
    idmsg = struct.pack("!hh", IDmsg, porta)    
    return idmsg

# Apesar do cliente somente receber respostas dos servents, 
# pode ser que mensagens erradas sejam enviadas a ele, e ele
# deve lê-las completamente pois o soquete é TCP
def leIDmsg(s):
    s.recv(2)
        
def leKeyReq(s):
    msg = s.recv(4)
    msg = s.recv(2)
    msgUnp = struct.unpack("!h", msg)
    tamanho = msgUnp[0]    
    msg = s.recv(tamanho)
    
def leTopoReq(s):
    s.recv(4)

def leFlood(s):
    msg = s.recv(12)             
    msg = s.recv(2)
    msgUnp = struct.unpack("!h", msg)
    tamanho = msgUnp[0]    
    msg = s.recv(tamanho)

def leResp(s, nseq):
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
    # Imprime a resposta recebida
    (ip_orig, porto_orig) = s.getpeername()      
    # Checa se nseq é valido
    if nseqRecv == nseq:
        print(valor+" "+str(ip_orig)+":"+str(porto_orig))  
        return True
    else:
        print("Mensagem incorreta recebida de "+str(ip_orig)+":"+str(porto_orig))
        return False
    
def consultaChave(c, nseq):
    # Cria mensagem de requisição de chave e envia para seu servent
    msg = criaKeyReq(c, nseq)
    csocket.send(msg)
    esperaResp(nseq)
    
def consultaTopologia(nseq):
    # Cria mensagem de requisição de topologia e envia para seu servent
    msg = criaTopoReq(nseq)
    csocket.send(msg)
    esperaResp(nseq)
    
def esperaResp(nseq):
    resp = False
    while True:
        try:
            # Espera conexões
            ssocket.settimeout(4)
            ssocket.listen()
            conn, addr = ssocket.accept()
            resp = recebeMsg(conn, nseq)   
        except socket.timeout:
            if not resp:
                print("Nenhuma resposta recebida")
            break
    
def recebeMsg(s, nseq):
    # Lê o tipo para saber como ler o restante
    msg = s.recv(2)
    if msg:
        msgUnp = struct.unpack("!h", msg)
        tipoRecv = msgUnp[0]    
        if tipoRecv == IDmsg:
            leIDmsg(s)        
        elif tipoRecv == KEYREQ:
            leKeyReq(s)    
        elif tipoRecv == TOPOREQ:
            leTopoReq(s)    
        elif tipoRecv == KEYFLOOD or tipoRecv == TOPOFLOOD:
            leFlood(s)    
        elif tipoRecv == RESP:
            valida = leResp(s, nseq)
            if valida:
                return True
        if tipoRecv != RESP:
            (ip_orig, porto_orig) = s.getpeername()
            print("Mensagem incorreta recebida de "+str(ip_orig)+":"+str(porto_orig))
    # Todas as outras mensagens são ignoradas
    return False   

def iniciaConsultas():
    nseq = 0
    for comando in sys.stdin:
        consulta = comando.split()        
        if consulta[0] == '?':
            # Consulta por Chave
            chave = consulta[1]
            consultaChave(chave, nseq)
            nseq += 1
        elif consulta[0] == 'T':
            # Consulta topologia
            consultaTopologia(nseq)
            nseq += 1
        elif consulta[0] == 'Q':
            # Finaliza o programa
            break
        else:
            print("Comando desconhecido")
            
''' Inicialização do programa
    Inicia conexão com o servent
    Envia mensagem de identificação para o servent (?) '''
    
if len(sys.argv) > 2:
    portaC = int(sys.argv[1])
    enderecoS, portaS = sys.argv[2].split(":")
else:
    print("Número de argumentos inválido.")
    print("-- Tente: <porto-local> <ip:porto>")
    sys.exit(1)
 
# Soquete para conectar com o servidor e enviar mensagens
csocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)        
endServidor = (enderecoS, int(portaS))
csocket.connect(endServidor)

# Soquete para aceitar conexões e receber mensagens
ssocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
ssocket.bind(("", portaC))

# Envia mensagem de ID para o servent
msg = criaIDmsg(portaC)
csocket.send(msg)

# Inicia loop para espera de comandos do usuario
try:
    iniciaConsultas()
except KeyboardInterrupt:
    csocket.close()
    ssocket.close()
    sys.exit(1)
