import os

def resolve_conflict(file_path):
    with open(file_path, 'r', encoding='utf-8-sig') as f:
        lines = f.readlines()
    
    start_index = -1
    end_index = -1
    
    for i, line in enumerate(lines):
        if '=======' in line:
            start_index = i + 1
        elif '>>>>>>> Stashed changes' in line:
            end_index = i
            break
            
    if start_index != -1 and end_index != -1:
        stashed_content = lines[start_index:end_index]
        with open(file_path, 'w', encoding='utf-8-sig') as f:
            f.writelines(stashed_content)
        print(f"Successfully resolved conflict in {file_path}")
    else:
        print(f"Could not find conflict markers in {file_path}")

if __name__ == "__main__":
    resolve_conflict('src/workspace/app_state/selections.json')
