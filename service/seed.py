from database import init_db, get_connection, update_sensor_status
import bct_client

SENSORS = [
    ("BCT_3D_4G_0207024", "E Peltason",    "Anteater",           "UCI"),
    ("BCT_VS_4G_0207024", "E Peltason",    "Anteater",           "UCI"), # Added in, was not present in CSV
    ("BCT_3D_4G_0207025", "E Peltason",    "Anteater",           "UCI"),
    ("BCT_3D_4G_0207032", "E Peltason",    "Pereira",            "UCI"),
    ("BCT_VS_4G_0207032", "E Peltason",    "Pereira",            "UCI"), # Added in, was not present in CSV
    ("BCT_3D_4G_0207034", "E Peltason",    "Pereira",            "UCI"),
    ("BCT_3D_4G_0207059", "E Peltason",    "Los Trancos",        "UCI"),
    ("BCT_3D_4G_0207031", "E Peltason",    "Engineering Service","UCI"),
    ("BCT_3D_4G_0207026", "W Peltason",    "Michael Drake",      "UCI"),
    ("BCT_3D_4G_0207028", "W Peltason",    "Pereira",            "UCI"),
    ("BCT_3D_4G_0207027", "W Peltason",    "Mesa",               "UCI"),
    ("BCT_3D_4G_0207030", "Health Sciences","Michael Drake",     "UCI"),
    ("BCT_3D_4G_0207035", "California",    "Michael Drake",      "UCI"),
    ("BCT_3D_4G_0207001", "Campus",        "Cornell",            "COI"),
    ("BCT_3D_4G_0207055", "Culver",        "University",         "COI"),
    # ("BCT_3D_4G_0207007", "Culver",        "University",         "COI") This was not coverd on the BCT Dashboard website
    ("BCT_VS_4G_0207002", "Culver",        "University",         "COI"),
    # ("BCT_3D_4G_0207002", "Culver",        "University",         "COI") Deprecated, replaced by BCT_3D_4G_0207055
    ("BCT_3D_4G_0207004", "University",    "Campus",             "COI"),
    ("BCT_3D_4G_0207006", "University",    "Campus",             "COI"),
    ("BCT_VS_4G_0207004", "University",    "Campus",             "COI"),
    ("BCT_3D_4G_0207059", "Campus",        "W Peltason",         "COI"),
    # ("BCT_3D_4G_0207009", "Campus",        "W Peltason",         "COI") Deprecated, replaced by BCT_3D_4G_0207057
    ("BCT_3D_4G_0207057", "Campus",        "W Peltason",         "COI"), # Added in, was not present in CSV
    ("BCT_3D_4G_0207003", "Campus",        "E Peltason",         "COI"),
    ("BCT_3D_4G_0207005", "Campus",        "E Peltason",         "COI"),
    ("BCT_VS_4G_0207003", "Campus",        "E Peltason",         "COI"), # This was not covered on the BCT Dashboard website
    ("BCT_3D_4G_0207008", "Culver",        "Campus",             "COI"),
    ("BCT_3D_4G_0207015", "Culver",        "Campus",             "COI"),
    ("BCT_3D_VS_0207008", "Culver",        "Campus",             "COI"),
    ("BCT_3D_4G_0207054", "Campus",        "California",         "COI"),
    ("BCT_3D_4G_0207018", "Campus",        "California",         "COI"),
    # ("BCT_3D_VS_0207017", "Campus",        "California",         "COI") Deprecated, replaced by BCT_3D_4G_0207054
    # ("BCT_3D_4G_0207017", "Campus",        "California",         "COI") Deprecated, replaced by BCT_3D_4G_0207054
    ("BCT_3D_4G_0207016", "Campus",        "Stanford",           "COI"), # the code says it's working but the BCD Dashboard website says its broken
    ("BCT_3D_4G_0207014", "Culver",        "Anteater",           "COI"),
    ("BCT_3D_4G_0207010", "Culver",        "Anteater",           "COI"),
    ("BCT_VS_4G_0207010", "Culver",        "Anteater",           "COI"),
    ("BCT_3D_4G_0207013", "Culver",        "Harvard",            "COI"),
    ("BCT_3D_4G_0207012", "Culver",        "Harvard",            "COI"),
    ("BCT_VS_4G_0207012", "Culver",        "Harvard",            "COI"),
    ("OBC-AG-SA-B972370", "Culver",        "Michelson",          "COI"),
    ("BCT_3D_4G_0207020", "Culver",        "Michelson",          "COI"),
    # ("BCT_3D_4G_0207037", "Culver",        "Michelson",          "COI") Deprecated, replaced by OBC-AG-SA-B972370
    ("BCT_3D_4G_0207011", "Unversity",        "Campus",    "COI"), # Used to be Culver, Vista del campo
    ("OBC-AG-SA-C251065",  "University",   "Harvard",            "COI"),
    # ("BCT_3D_4G_0207036", "University",    "Harvard",            "COI") Deprecated, replaced by OBC-AG-SA-C251065
    # ("BCT_3D_4G_0207029", "University",    "Harvard",            "COI") Deprecated, replaced by OBC-AG-SA-C251065
    # ("BCT_VS_4G_0207029", "University",    "Harvard",            "COI") Deprecated, replaced by OBC-AG-SA-C251065
    ("BCT_3D_4G_0207021", "University",    "California",         "COI"),
    ("BCT_3D_4G_0207022", "University",    "Mesa",               "COI"),
    ("BCT_3D_4G_0207023", "Harvard",       "Bridge",             "COI"),
]


if __name__ == "__main__":
    init_db()
    with get_connection() as conn:
        conn.executemany(
            "INSERT OR REPLACE INTO sensors (udid, major, minor, authority) VALUES (?, ?, ?, ?)",
            SENSORS,
        )
    
    for sensor in SENSORS:
        update_sensor_status(sensor[0], bct_client.get_status(sensor[0]))

    print(f"Seeded {len(SENSORS)} sensors.")
