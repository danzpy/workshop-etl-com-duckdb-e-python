import pandas as pd
import numpy as np
import json
from pathlib import Path

# Data template
data_template = {
    'aluno': ['Jose', 'Arnaldo','','Gabriel', 'Maria', 'Joao', 'Fernanda', 'Lucas', 'Ana', 'Rafael', 'Maria', 'Carla'],
    'materia': ['Artes', 'Matematica','Historia', 'Ciencias', 'Portugues', 'Ciencias', 'Ciencias', 'Ciencias', 'Ciencias', 'Ciencias', 'Historia', 'Historia'],
    'nota': [67, 94, 93, 91, 98, 96, 55, 78, 62, 92],
    'periodo': ['Noite', 'Tarde', 'Noite', 'Tarde', 'Noite', 'Noite', 'Manha', 'Noite', 'Manha', 'Noite']
}

# Function to create random data based on the template
def create_random_data(template, num_entries):
    random_data = {
        'aluno': np.random.choice(template['aluno'], num_entries),
        'materia': np.random.choice(template['materia'], num_entries),
        'nota': np.random.randint(50, 100, num_entries),  # Random notes between 50 and 100
        'periodo': np.random.choice(template['periodo'], num_entries)
    }
    return pd.DataFrame(random_data)

# Create 5 random parquet files
num_entries = 10  # Number of entries per file
output_dir = Path('./parquet_files')
output_dir.mkdir(parents=True, exist_ok=True)

for i in range(1, 6):
    df = create_random_data(data_template, num_entries)
    file_path = output_dir / f'data_{i}.parquet'
    df.to_parquet(file_path, engine='pyarrow')

print(f'Parquet files created in {output_dir}')

#################################################################################################################

# Create 5 random JSON files
num_entries = 10  # Number of entries per file
output_dir = Path('./json_files')
output_dir.mkdir(parents=True, exist_ok=True)

file_paths = []

for i in range(1, 6):
    df = create_random_data(data_template, num_entries)
    file_path = output_dir / f'data_{i}.json'
    df.to_json(file_path, orient='records', lines=True)
    file_paths.append(file_path)

print(f'Json files created in {output_dir}')