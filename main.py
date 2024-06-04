#!/usr/bin/python

import sys
import os

act_dir = os.path.dirname(os.path.abspath(__file__))
proyect_dir = os.path.join(act_dir, 'src')
sys.path.append(proyect_dir)

from src.utils import *
from src.telegram_bot import enviar_mensaje

if __name__ == "__main__":
    asyncio.run(enviar_mensaje('Tercer_anillo_claro'))
    load('tb_etiquetados_tercer_anillo_claro', engine_4(), 'append', False, transform())
    asyncio.run(enviar_mensaje("____________________________________"))
    