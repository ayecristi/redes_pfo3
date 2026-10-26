import socket
import json
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from werkzeug.security import generate_password_hash, check_password_hash

# NOTA ARQUITECTURA: Para facilitar la ejecución local de este prototipo, 
# se utiliza SQLite. En el entorno productivo (según diagrama), 
# esta conexión se realiza mediante psycopg2 hacia PostgreSQL.

# Configuración del servidor Socket
HOST = '127.0.0.1'
PORT = 65432
BUFFER_SIZE = 4096
MAX_WORKERS = 10  # Pool de hilos para manejar clientes/tareas concurrentes

DB_FILE = 'database.db'
db_lock = threading.Lock() # Necesario en SQLite para evitar colisiones entre hilos

def init_db():
    """Inicializa la base de datos (En la arquitectura real, esto es PostgreSQL)"""
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS usuarios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tareas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario_id INTEGER NOT NULL,
                descripcion TEXT NOT NULL,
                estado TEXT DEFAULT 'pendiente',
                FOREIGN KEY (usuario_id) REFERENCES usuarios (id)
            )
        ''')
        conn.commit()

def procesar_peticion(payload):
    """
    Simula el enrutamiento del servidor.
    En una arquitectura completa, las tareas de escritura pesadas se enviarían a RabbitMQ acá.
    """
    accion = payload.get('accion')
    datos = payload.get('datos', {})

    if accion == 'registro':
        return manejar_registro(datos)
    elif accion == 'login':
        return manejar_login(datos)
    elif accion == 'crear_tarea':
        return manejar_crear_tarea(datos)
    elif accion == 'ver_tareas':
        return manejar_ver_tareas(datos)
    else:
        return {"status": "error", "mensaje": "Acción no reconocida"}

def manejar_registro(datos):
    usuario = datos.get('usuario')
    password = datos.get('password')
    if not usuario or not password:
        return {"status": "error", "mensaje": "Faltan credenciales"}
    
    hashed_pw = generate_password_hash(password)
    try:
        with db_lock, sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute('INSERT INTO usuarios (usuario, password_hash) VALUES (?, ?)', (usuario, hashed_pw))
            conn.commit()
        return {"status": "success", "mensaje": "Usuario registrado exitosamente"}
    except sqlite3.IntegrityError:
        return {"status": "error", "mensaje": "El usuario ya existe"}

def manejar_login(datos):
    usuario = datos.get('usuario')
    password = datos.get('password')
    with db_lock, sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT id, password_hash FROM usuarios WHERE usuario = ?', (usuario,))
        row = cursor.fetchone()
        
    if row and check_password_hash(row[1], password):
        return {"status": "success", "mensaje": "Login exitoso", "usuario_id": row[0]}
    return {"status": "error", "mensaje": "Credenciales inválidas"}

def manejar_crear_tarea(datos):
    # NOTA ARQUITECTURA: Esto idealmente publicaría un mensaje en RabbitMQ
    usuario_id = datos.get('usuario_id')
    descripcion = datos.get('descripcion')
    with db_lock, sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('INSERT INTO tareas (usuario_id, descripcion) VALUES (?, ?)', (usuario_id, descripcion))
        conn.commit()
    return {"status": "success", "mensaje": "Tarea encolada/creada exitosamente"}

def manejar_ver_tareas(datos):
    usuario_id = datos.get('usuario_id')
    with db_lock, sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT id, descripcion, estado FROM tareas WHERE usuario_id = ?', (usuario_id,))
        tareas = [{"id": row[0], "descripcion": row[1], "estado": row[2]} for row in cursor.fetchall()]
    return {"status": "success", "tareas": tareas}

def handle_client(conn, addr):
    """Función ejecutada por el ThreadPool para cada cliente conectado"""
    print(f"[NUEVA CONEXIÓN] Cliente conectado desde {addr}")
    try:
        while True:
            data = conn.recv(BUFFER_SIZE)
            if not data:
                break
            
            # Decodificar el mensaje del cliente
            mensaje_str = data.decode('utf-8')
            payload = json.loads(mensaje_str)
            print(f"[RECV] de {addr}: {payload.get('accion')}")
            
            # Procesar y responder
            respuesta = procesar_peticion(payload)
            conn.sendall(json.dumps(respuesta).encode('utf-8'))
            
    except ConnectionResetError:
        print(f"[DESCONEXIÓN ABRUPTA] {addr}")
    except json.JSONDecodeError:
        error_msg = json.dumps({"status": "error", "mensaje": "Formato JSON inválido"})
        conn.sendall(error_msg.encode('utf-8'))
    finally:
        conn.close()
        print(f"[DESCONECTADO] {addr}")

def iniciar_servidor():
    init_db()
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    # El secreto: hacer que el socket respire cada 1 segundo
    server.settimeout(1.0) 
    
    server.bind((HOST, PORT))
    server.listen()
    
    print(f"[INICIANDO] Servidor TCP escuchando en {HOST}:{PORT}")
    print(f"[ARQUITECTURA] Usando ThreadPoolExecutor con {MAX_WORKERS} workers")
    
    # Evitamos que el pool trabe la salida con shutdown
    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    try:
        while True:
            try:
                conn, addr = server.accept()
                executor.submit(handle_client, conn, addr)
            except socket.timeout:
                # No se conectó nadie en este segundo, volvemos a intentar
                pass
    except KeyboardInterrupt:
        print("\n[APAGANDO] Capturado Ctrl+C. Cerrando el servidor...")
    finally:
        executor.shutdown(wait=False)
        server.close()
        print("[APAGADO] Servidor cerrado correctamente.")

if __name__ == "__main__":
    iniciar_servidor()