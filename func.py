import oci
import csv
import cx_Oracle
import io
import zipfile
import gzip
import os
import json
import sys
import logging
logging.basicConfig(level=logging.INFO)
from fdk import response


def handler(ctx, data: io.BytesIO=None):
    signer = oci.auth.signers.get_resource_principals_signer()
    body = json.loads(data.getvalue())
    #print("Event Details: {} ".format(body), flush=True)
    print("INFO - Event ID {} received".format(body["eventID"]), flush=True)
    print("INFO - Object name: " + body["data"]["resourceName"], flush=True)
    object_name = body["data"]["resourceName"]
    print("INFO - Bucket name: " + body["data"]["additionalDetails"]["bucketName"], flush=True)
    bucket_name = body["data"]["additionalDetails"]["bucketName"]
    namespace = body["data"]["additionalDetails"]["namespace"]
    resp = do(signer, namespace, object_name, bucket_name)
    return response.Response(ctx,response_data=json.dumps(resp),headers={"Content-Type": "application/json"})

# INSERT data into ADB using executemany
def persistSalesData(connection, data):
    sql = ('insert into W_JDA_JDASAL_FS_2 values(:1 ,:2 ,:3 ,:4 ,:5 ,:6 ,:7 ,:8 ,:9 ,:10 ,:11 ,:12 ,:13 ,:14 ,:15 ,:16 ,:17 ,:18 ,:19 ,:20 ,:21 ,:22 ,:23 ,:24 ,:25 ,:26 ,:27 ,:28 ,:29 ,:30 ,:31 ,:32 ,:33 ,:34 ,:35 ,:36 ,:37 ,:38 ,:39 ,:40 ,:41 ,:42 ,:43 ,:44 ,:45 ,:46 ,:47 ,:48 ,:49 ,:50 ,:51 ,:52 ,:53 ,:54 ,:55 ,:56 ,:57 ,:58 ,:59 ,:60 ,:61 ,:62 ,:63 ,:64 ,:65 ,:66 ,:67 ,:68 ,:69 ,:70 ,:71 ,:72 ,:73 ,:74 ,:75 ,:76 ,:77 ,:78 ,:79 ,:80 ,:81 ,:82 ,:83 ,:84 ,:85 ,:86 ,:87 ,:88 ,:89 ,:90 ,:91 ,:92 ,:93 ,:94 ,:95 ,:96 ,:97 ,:98 ,:99 ,:100 ,:101 ,:102 ,:103 ,:104 ,:105 ,:106 ,:107 ,:108 ,:109 ,:110 ,:111 ,:112 ,:113 ,:114 ,:115 ,:116 ,:117 ,:118 ,:119 ,:120 ,:121 ,:122 ,:123 ,:124 ,:125 ,:126 ,:127 ,:128 ,:129 ,:130 ,:131)')

    try:
        # create a cursor
        with connection.cursor() as cursor:
            # execute the insert statement
            cursor.prepare(sql)
            cursor.executemany(None, data)
            # commit work
            connection.commit()
            return "success"

    except cx_Oracle.Error as error:
        print('SQL Error occurred:')
        print(error)
        return "failed"


def move_object(signer, namespace, source_bucket, destination_bucket, object_name):
    object = oci.object_storage.ObjectStorageClient(config={}, signer=signer)
    #object = oci.object_storage.ObjectStorageClient(config)
    object_composite_ops = oci.object_storage.ObjectStorageClientCompositeOperations(object)
    print("Started copying object from {0} to {1}".format(source_bucket, destination_bucket), flush=True)
    try:
        resp = object_composite_ops.copy_object_and_wait_for_state(
            namespace,
            source_bucket,
            oci.object_storage.models.CopyObjectDetails(
                destination_bucket=destination_bucket,
                destination_namespace=namespace,
                destination_object_name=object_name,
                destination_region=signer.region,
                source_object_name=object_name
            ),
            wait_for_states=[
                oci.object_storage.models.WorkRequest.STATUS_COMPLETED,
                oci.object_storage.models.WorkRequest.STATUS_FAILED])
    except Exception as e:
        print("Failure: " + str(e), flush=True)
    #print(resp.data)

    if resp.data.status != "COMPLETED":
        raise Exception("cannot copy object {0} to bucket {1}".format(object_name, destination_bucket))
    else:
        resp = object.delete_object(namespace, source_bucket, object_name)
        print("INFO - Object {0} has been moved to Bucket {1}".format(object_name, destination_bucket), flush=True)

def do(signer, namespace, object_name, bucket_name):
    try:
        object_storage = oci.object_storage.ObjectStorageClient(config={}, signer=signer)
        autonomous_db = oci.database.DatabaseClient({}, signer=signer)
        #namespace = object_storage.get_namespace().data
        #bucket_name = os.environ.get("bucket")
        os.environ["TNS_ADMIN"] = "/tmp/wallet"

        #wallet location
        wallet_path = '/tmp/wallet'
        # autonomous database dependencies..
        autonomous_database_id = os.environ['db_ocid']

        generate_autonomous_database_wallet_details = oci.database.models.GenerateAutonomousDatabaseWalletDetails(password='YourPassword')
        # create local directories..
        if not os.path.exists(wallet_path):
            os.mkdir(wallet_path)

        # download db client credential package..
        with open(wallet_path + '/' + 'wallet.zip', 'wb') as f:
            wallet_details = autonomous_db.generate_autonomous_database_wallet(autonomous_database_id,
                                                                               generate_autonomous_database_wallet_details)
            for chunk in wallet_details.data.raw.stream(1024 * 1024, decode_content=False):
                f.write(chunk)
                logging.info('finished downloading ' + wallet_path + '/' + 'wallet.zip')

            # extract..
            with zipfile.ZipFile(wallet_path + '/' + 'wallet.zip', 'r') as zip_obj:
                zip_obj.extractall(wallet_path)

            # update sqlnet.ora..
            with open(wallet_path + '/sqlnet.ora', 'r') as sqlnet_file:
                sqlnet_filedata = sqlnet_file.read()
            sqlnet_filedata = sqlnet_filedata.replace('?/network/admin', '/tmp/wallet')
            with open(wallet_path + '/sqlnet.ora', 'w') as sqlnet_file:
                sqlnet_file.write(sqlnet_filedata)

        # Create DB connection based on my DB details
        connection = cx_Oracle.connect('Username/YourPassword@DBNAME_high')
        #Get object name
        all_objects = object_storage.list_objects(namespace, bucket_name).data
        #for new in range(len(all_objects.objects)):
        # Get object name
        #object_name = all_objects.objects[new].name
        #print(object_name)
        #logging.info(object_name)
        if str(object_name).lower()[-3:] == 'csv':
            # Get object name and contents of object
            object = object_storage.get_object(namespace, bucket_name, object_name)
            # object.data.text is string..we need splitlines
            batch_data = csv.reader(object.data.text.splitlines())
            # Skip the first row as it's header
            next(batch_data)
            data = list(batch_data)
            #print(data)
            # for row in batch_data:
            # print(row)
            status = persistSalesData(connection, data)
            if status == "success":
                print("INFO - Data has been inserted in Autonomous Database", flush=True)
                # Move to another bucket
                destination_bucket = bucket_name + "_completed"
                move_object(signer, namespace, bucket_name, destination_bucket, object_name)
            else:
                print("Failed to insert data with {}".format(object_name), flush=True)
        else:
            print("no action is required.", flush=True)

    except Exception as e:
        print("Failure: " + str(e), flush=True)
