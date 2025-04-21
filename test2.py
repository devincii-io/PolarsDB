from src.client import DBClient
from src.config import DBConfig

def main():
    config = DBConfig(config_path="config.json"	)
    client = DBClient(config)

    df = client.read_table("customers")
    print(df)

if __name__ == "__main__":
    main()

    
