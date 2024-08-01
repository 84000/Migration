
from supabase import create_client,Client



# get environment variables
url: str = ""
key: str = ""
supabase = create_client(url, key)

def make_title(work,WorkId):
    titles = work['work'][0]['title']
    titles_dicts = [dict({'work_uuid': WorkId,
                          'type': title['titleType'],
                          'content': title['content'],
                          'language': title['language'],
                          'migrationId': title['titleMigrationId']}) for title in titles]
    return titles_dicts


def make_passages(work,WorkId):
    # check if work has translation
    if 'translation' not in work:
        return None
    passages = work['translation']['passage']
    passage_dict = [dict({'work_uuid': WorkId,
                          'xmlId': passage['xmlId'],
                          'parent': passage['parentId'],
                          'label': passage['passageLabel'],
                          'type': passage['segmentationType'],
                          'sort': passage['passageSort'],
                          'content': passage['content']}) for passage in passages]
    return passage_dict

def make_work(translation):
    title = translation['work'][0]['title'][0]['content']
    Current_Work = translation['work'][0]

    work_dict = dict({'xmlId': Current_Work['workId'],
                      'url': Current_Work['url'],
                      'type': Current_Work['workType'],
                      'toh': Current_Work['catalogueWorkIds'],
                      'publicationDate': translation['publicationDate'],
                     'publicationStatus': translation['publicationStatus'],
                    'publicationVersion': translation['publicationVersion'],
                      'title': title,
                      'migrationJson': translation})
    return work_dict


# upsert into supabase
def store_work(work_dict):
    # check if the work already exists with the same xmlId
    # if exists return the workId
    # else insert the work and return the workId
    does = supabase.table('works').select('uuid').eq('xmlId',work_dict['xmlId']).execute()
    if (len(does.data)!=0):
        print("⚠️ Work Already Exists, Delete Work and its Titles and Passages")
        # delete the work and all its titles and passages
        data_temp,count_temp = supabase.table('works').delete().eq('uuid',does.data[0]['uuid']).execute()
    try:
        work_data,count = supabase.table('works').insert(work_dict).execute()
        return work_data[1][0]['uuid']
    except Exception as e:
        print ("Error Occured",e)
        return None


def store_titles(titles_dicts):
    # check if titles already exists with the same migrationId
    try:
        title_data,count = supabase.table('titles').upsert(titles_dicts).execute()
        print("✅ Added titles",len(title_data[1]))
        return "success"
    except Exception as e:
        print("Error Occured",e)
        return e
        # If error occurs delete all just inserted data
        # try:
        #     if (title_data[1]):
        #         for inserts in title_data[1]:
        #             print(inserts)
        #             data_temp,count_temp = supabase.table('titles').delete().eq('id',inserts['id']).execute()
        #             print('deleted',data_temp[1])
        # except IndexError:
        #     print("No Data to Delete")

def store_passages(passage_dict):
    try:
        passage_data,count = supabase.table('passages').upsert(passage_dict).execute()
        print("✅ Added passages",len(passage_data[1]))
        return "success"
    except Exception as e:
        print("Error Occured",e)
        return e
        # If error occurs delete all just inserted data
        # try:
        #     if (passage_data[1]):
        #         for inserts in passage_data[1]:
        #             print(inserts)
        #             data_temp,count_temp = supabase.table('passages').delete().eq('id',inserts['id']).execute()
        #             print('deleted',data_temp[1])
        # except IndexError:
        #     print("No Data to Delete")


def load_data(translation):
    try:
        work_uuid = None
        WorkId = translation['work'][0]['workId']
        work_dict = make_work(translation)
        print("☁️ Storing Work")
        work_uuid = store_work(work_dict)
        if (work_uuid != None):
            print("Work UUID",work_uuid)
            print("Loading Titles")
            titles_dicts = make_title(translation,work_uuid)
            print("Loading Passages")
            passage_dict = make_passages(translation,work_uuid)
            print("☁️ Storing Title")
            return_titles = store_titles(titles_dicts)
            if (return_titles != "success"):
                raise Exception(return_titles)
            if (passage_dict):
                print("Storing Passages")
                return_passages = store_passages(passage_dict)
                if (return_passages != "success"):
                    raise Exception(return_passages)
            else:
                print("❌ No Passages to Store")
    except Exception as e:
        #delete work cascade
        try:
            print('‼️ error in',WorkId)
            # store error as string
            supabase.table('Error').upsert({'error':str(e),'xmlId':WorkId}).execute()
            if (work_uuid):
                data_temp,count_temp = supabase.table('works').delete().eq('uuid',work_uuid).execute()
                print('deleted',work_uuid)
        except IndexError:
            print("No Data to Delete")

    return