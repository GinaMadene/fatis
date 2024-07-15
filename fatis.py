import pymysql

# Connexion à la BaseDeDonneesSource (pour récupérer les données)
conn_source = pymysql.connect(
    host="localhost",
    user="root",
    password="",
    database="AncienneBaseDeDonnees"
)

# Connexion à la BaseDeDonneesDestination (pour insérer les données)
conn_dest = pymysql.connect(
    host="localhost",
    user="root",
    password="",
    database="NouvelleBaseDeDonnees"
)

cursor_source = conn_source.cursor()
cursor_dest = conn_dest.cursor()

try:
    # Récupérer les données de AncienneTableA de AncienneBaseDeDonnees
    cursor_source.execute("SELECT nom FROM AncienneTableA")
    rows_tableA = cursor_source.fetchall()

    # Récupérer les données de AncienneTableB de AncienneBaseDeDonnees
    cursor_source.execute("SELECT description FROM AncienneTableB")
    rows_tableB = cursor_source.fetchall()

    # Méthode 1 : Supprimer toutes les données de NouvelleTableC avant d'insérer de nouvelles données
    cursor_dest.execute("DELETE FROM NouvelleTableC")

    # Insertion des données récupérées dans NouvelleTableC de NouvelleBaseDeDonnees
    for nom, description in zip(rows_tableA, rows_tableB):
        cursor_dest.execute("INSERT INTO NouvelleTableC (nom, description) VALUES (%s, %s)", (nom[0], description[0]))

    conn_dest.commit()
    print("Insertion de données réussie dans NouvelleTableC de NouvelleBaseDeDonnees")

    # Afficher les données insérées dans NouvelleTableC pour vérification
    cursor_dest.execute("SELECT * FROM NouvelleTableC")
    rows_tableC = cursor_dest.fetchall()

    print("Données insérées dans NouvelleTableC de NouvelleBaseDeDonnees:")
    for row in rows_tableC:
        print(row)

except pymysql.MySQLError as err:
    print(f"Erreur MySQL : {err}")

finally:
    # Fermeture des connexions
    cursor_source.close()
    conn_source.close()
    cursor_dest.close()
    conn_dest.close()
