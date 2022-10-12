import yaml

class DomainPipelineConfig():
  """
  Helper class for containing configuration items.

  Makes things a bit cleaner than nested dictionaries everywhere in the rest of the code.
  """
  # Raw config items
  config_raw: dict
  config_path: str

  metadata_log_table: str

  extract_log_path: str
  extract_cities: list
  extract_apis: dict

  load_mode: str
  load_chunksize: int
  load_key_columns: dict

  transform_model_path: str

  def __init__(self,
      config_path: str
  ):
      self.config_path = config_path

      if (self.config_path != ""):
          with open(self.config_path) as file_stream:
              self.config_raw = yaml.safe_load(file_stream)

      self.metadata_log_table = self.config_raw["meta"]["log_table"]
      self.extract_log_path = self.config_raw["extract"]["log_path"]
      self.extract_cities = self.config_raw["extract"]["target_cities"]
      self.extract_apis = self.config_raw["extract"]["api_urls"]
      self.load_mode = self.config_raw["load"]["mode"]
      self.load_chunksize = int(self.config_raw["load"]["chunksize"])
      self.load_key_columns = self.config_raw["load"]["key_columns"]
      self.transform_model_path = self.config_raw["transform"]["model_path"]
