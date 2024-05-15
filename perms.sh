hdfs dfs -setfacl -R -m user:adm_209_nyu_edu:rwx gbr_write.csv
hdfs dfs -setfacl -R -m default:user:adm_209_nyu_edu:rwx gbr_write.csv
hdfs dfs -setfacl -R -m user:as18774_nyu_edu:rwx gbr_write.csv
hdfs dfs -setfacl -R -m default:user:as18774_nyu_edu:rwx gbr_write.csv
hdfs dfs -setfacl -R -m user:as18464_nyu_edu:rwx gbr_write.csv
hdfs dfs -setfacl -R -m default:user:as18464_nyu_edu:rwx gbr_write.csv

hdfs dfs -setfacl -R -m user:adm_209_nyu_edu:rwx game_write.csv
hdfs dfs -setfacl -R -m default:user:adm_209_nyu_edu:rwx game_write.csv
hdfs dfs -setfacl -R -m user:as18774_nyu_edu:rwx game_write.csv
hdfs dfs -setfacl -R -m default:user:as18774_nyu_edu:rwx game_write.csv
hdfs dfs -setfacl -R -m user:as18464_nyu_edu:rwx game_write.csv
hdfs dfs -setfacl -R -m default:user:as18464_nyu_edu:rwx game_write.csv

hdfs dfs -setfacl -R -m user:adm_209_nyu_edu:rwx player_write.csv
hdfs dfs -setfacl -R -m default:user:adm_209_nyu_edu:rwx player_write.csv
hdfs dfs -setfacl -R -m user:as18774_nyu_edu:rwx player_write.csv
hdfs dfs -setfacl -R -m default:user:as18774_nyu_edu:rwx player_write.csv
hdfs dfs -setfacl -R -m user:as18464_nyu_edu:rwx player_write.csv
hdfs dfs -setfacl -R -m default:user:as18464_nyu_edu:rwx player_write.csv

hdfs dfs -setfacl -R -m user:adm_209_nyu_edu:rwx ref_write.csv
hdfs dfs -setfacl -R -m default:user:adm_209_nyu_edu:rwx ref_write.csv
hdfs dfs -setfacl -R -m user:as18774_nyu_edu:rwx ref_write.csv
hdfs dfs -setfacl -R -m default:user:as18774_nyu_edu:rwx ref_write.csv
hdfs dfs -setfacl -R -m user:as18464_nyu_edu:rwx ref_write.csv
hdfs dfs -setfacl -R -m default:user:as18464_nyu_edu:rwx ref_write.csv

hdfs dfs -setfacl -R -m user:adm_209_nyu_edu:rwx ref_player
hdfs dfs -setfacl -R -m default:user:adm_209_nyu_edu:rwx ref_player
hdfs dfs -setfacl -R -m user:as18774_nyu_edu:rwx ref_player
hdfs dfs -setfacl -R -m default:user:as18774_nyu_edu:rwx ref_player
hdfs dfs -setfacl -R -m user:as18464_nyu_edu:rwx ref_player
hdfs dfs -setfacl -R -m default:user:as18464_nyu_edu:rwx ref_player

hdfs dfs -setfacl -R -m user:adm_209_nyu_edu:rwx ref_org
hdfs dfs -setfacl -R -m default:user:adm_209_nyu_edu:rwx ref_org
hdfs dfs -setfacl -R -m user:as18774_nyu_edu:rwx ref_org
hdfs dfs -setfacl -R -m default:user:as18774_nyu_edu:rwx ref_org
hdfs dfs -setfacl -R -m user:as18464_nyu_edu:rwx ref_org
hdfs dfs -setfacl -R -m default:user:as18464_nyu_edu:rwx ref_org

hdfs dfs -setfacl -R -m user:adm_209_nyu_edu:rwx player_stats
hdfs dfs -setfacl -R -m default:user:adm_209_nyu_edu:rwx player_stats
hdfs dfs -setfacl -R -m user:as18774_nyu_edu:rwx player_stats
hdfs dfs -setfacl -R -m default:user:as18774_nyu_edu:rwx player_stats
hdfs dfs -setfacl -R -m user:as18464_nyu_edu:rwx player_stats
hdfs dfs -setfacl -R -m default:user:as18464_nyu_edu:rwx player_stats

hdfs dfs -setfacl -R -m user:adm_209_nyu_edu:rwx org_stats
hdfs dfs -setfacl -R -m default:user:adm_209_nyu_edu:rwx org_stats
hdfs dfs -setfacl -R -m user:as18774_nyu_edu:rwx org_stats
hdfs dfs -setfacl -R -m default:user:as18774_nyu_edu:rwx org_stats
hdfs dfs -setfacl -R -m user:as18464_nyu_edu:rwx org_stats
hdfs dfs -setfacl -R -m default:user:as18464_nyu_edu:rwx org_stats

hdfs dfs -setfacl -R -m user:adm_209_nyu_edu:rwx player_diff
hdfs dfs -setfacl -R -m default:user:adm_209_nyu_edu:rwx player_diff
hdfs dfs -setfacl -R -m user:as18774_nyu_edu:rwx player_diff
hdfs dfs -setfacl -R -m default:user:as18774_nyu_edu:rwx player_diff
hdfs dfs -setfacl -R -m user:as18464_nyu_edu:rwx player_diff
hdfs dfs -setfacl -R -m default:user:as18464_nyu_edu:rwx player_diff

hdfs dfs -setfacl -R -m user:adm_209_nyu_edu:rwx org_diff
hdfs dfs -setfacl -R -m default:user:adm_209_nyu_edu:rwx org_diff
hdfs dfs -setfacl -R -m user:as18774_nyu_edu:rwx org_diff
hdfs dfs -setfacl -R -m default:user:as18774_nyu_edu:rwx org_diff
hdfs dfs -setfacl -R -m user:as18464_nyu_edu:rwx org_diff
hdfs dfs -setfacl -R -m default:user:as18464_nyu_edu:rwx org_diff

hdfs dfs -getfacl gbr_write.csv
hdfs dfs -getfacl game_write.csv
hdfs dfs -getfacl player_write.csv
hdfs dfs -getfacl ref_write.csv
hdfs dfs -getfacl ref_player
hdfs dfs -getfacl ref_org
hdfs dfs -getfacl org_stats
hdfs dfs -getfacl player_diff
hdfs dfs -getfacl org_diff