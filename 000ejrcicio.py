# -*- coding: utf-8 -*-
"""
Editor de Spyder

Este es un archivo temporal.
"""
dias = 1
billetes = 0.011
while billetes < 67.5:
    billetes = billetes*2
    dias += 1
print(dias)

cadena = 'extrangero'

def geringoso(palabra):
    cadena_geringosa = ''
    for c in palabra:
        if c == 'a':
            cadena_geringosa += 'apa'
        elif c == 'e':
            cadena_geringosa += 'epe'
        elif c == 'i':
            cadena_geringosa += 'ipi'
        elif c == 'o':
            cadena_geringosa += 'opo'
        elif c == 'u':
            cadena_geringosa += 'upu'
        else:
            cadena_geringosa += c
    return cadena_geringosa
        
print(geringoso(cadena))


lista = [1,2,3,4,5]
lista2 = [1,2,3,4]
def pertenece(lista, elem):
    for e in lista:
        if e == elem:
            return True
    return False
pertenece(lista, 6)

def mas_larga(lista1, lista2):
    if len(lista1) > len(lista2):
        return lista1
    elif len(lista2) > len(lista1):
        return lista2
    else:
        return lista1,lista2
mas_larga(lista, lista2)

inicio = 100
rebotes = 0
tabla = "Rebote: " + str(rebotes) + " | " + "Altura: " + str(inicio) + "\n"
while rebotes < 10:
    inicio = (3*inicio)/5
    rebotes += 1
    tabla += "Rebote: " + str(rebotes) + " | " + "Altura: " + str(inicio) + "\n"

print(tabla)

cadena1 = "pepepona"
cadena2 = "jose"
def mezclar(cadena1, cadena2):
    largo = len(cadena1)
    mezcla = ""
    if len(cadena1) > len(cadena2):
        largo = len(cadena1)
        for i in range(0,largo,1):
            mezcla += cadena1[i]
            if i < len(cadena2):
               mezcla += cadena2[i]
    elif len(cadena2) > len(cadena1):
        largo = len(cadena2)
        for i in range(0,largo,1):
            if i < len(cadena1):
                mezcla += cadena1[i]
            mezcla += cadena2[i]
    else:
        for i in range(0,largo,1):
            mezcla += cadena1[i]+cadena2[i]
    return mezcla

print(mezclar(cadena1, cadena2))


lista = ['banana', 'manzana', 'mandarina']
def traductor_geringoso(lista):
    dicio = {}
    for l in lista:
        dicio[l] = geringoso(l)
    return dicio

print(traductor_geringoso(lista))






















