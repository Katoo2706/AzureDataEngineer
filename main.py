from source.azure_adls import AzureStorage

if __name__ == "__main__":
    AzureStorage(storage_account="databrickcourseextdl",
                 access_token="20Nasa6Jc9NxIAggWyXpU5WQD2N1eOU2QSPcnsXp9pyBMgt6SpkgZ6N8l1hjG2aH3L++QS9ZQKsW+AStZZgSGg=="
                 ).upload_file_blob(
        container_name="bronze",
        local_path="./unity-catalog-resource",
        local_file_name="drivers.json"
    )