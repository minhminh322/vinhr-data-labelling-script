from pathlib import Path

class Config():
    def __init__(self, root, round_nums = 1):
        self.root = root
        self.round_nums = round_nums
        self.xou_value_input = 0.2
        self.label_1_path = Path(f'{root}/In/1_labels/Team1/')
        self.label_2_path = Path(f'{root}/In/1_labels/Team2/')
        self.mapping_path = Path(f'{root}/Mapping_round_{round_nums}.csv')
        self.dest_path =  Path(f'{root}/Out/1_merged/')
        self.merge_path_fb = Path(f'{root}/Out/1_merged/')
        self.feedback_1_path = Path(f'{root}/Out/1_feedback/Team1/')
        self.feedback_2_path = Path(f'{root}/Out/1_feedback/Team2/')
        self.dest_path_fb = Path(f'{root}/Out/1_feedback/')
        self.label_1_final_path = Path(f'{root}/In/2_labels/Team1/')
        self.label_2_final_path = Path(f'{root}/In/2_labels/Team2/')
        self.dest_final_path = Path(f'{root}/Out/2_merged/')
        
class config_gen_data_label():
    def __init__(self, root_round_1, root_round_2):
        self.data_root_path = Path(f'{root_round_2}/In/watches/')
        self.label_round1_path = Path(f'{root_round_1}/Out/2_merged/')
        self.label_root_path = Path(f'{root_round_2}/Out/2_merged/')
        self.dest_path = Path(f'{root_round_2}/Out/dataset/')

        self.report_mapping_path = Path(f'{root_round_2}/Mapping_round_2.csv')

        