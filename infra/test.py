import os
local_folder = '../input_data/starter'
s3_folder = 'infinity_works'
walks = os.walk(local_folder)
for source, dirs, files in walks:
    print("----------")
    print("SOURCE", source)
    print("----------")
    print("DIRS", dirs)
    print("----------")
    print("FILES", files)



