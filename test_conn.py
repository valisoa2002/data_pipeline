import pg8000
import traceback

password = "airflow"
print("Mot de passe utilisé (repr):", repr(password))
print("Octets:", password.encode("utf-8"))
print("Longueur:", len(password))

print("\nTentative de connexion via pg8000...")
try:
    conn = pg8000.connect(
        host="127.0.0.1",
        port=5434,
        user="airflow",
        password=password,
        database="airflow",
        timeout=5,
    )
    print("CONNEXION OK")
    cur = conn.cursor()
    cur.execute("SELECT current_user, current_database();")
    print(cur.fetchone())
    conn.close()
except Exception:
    print("ERREUR COMPLETE :")
    traceback.print_exc()