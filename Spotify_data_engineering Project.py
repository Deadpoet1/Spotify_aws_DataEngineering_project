import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Artist
Artist_node1715153831394 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://snowflakebucketsaif/csv1_awsproject/spotify_artist_data_2023.csv"], "recurse": True}, transformation_ctx="Artist_node1715153831394")

# Script generated for node Tracks
Tracks_node1715153898055 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://snowflakebucketsaif/csv1_awsproject/spotify_tracks_data_2023.csv"], "recurse": True}, transformation_ctx="Tracks_node1715153898055")

# Script generated for node Album
Album_node1715153897177 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://snowflakebucketsaif/csv1_awsproject/spotify-albums_data_2023.csv"], "recurse": True}, transformation_ctx="Album_node1715153897177")

# Script generated for node Join-Album-Artist
JoinAlbumArtist_node1715154429502 = Join.apply(frame1=Artist_node1715153831394, frame2=Album_node1715153897177, keys1=["id"], keys2=["artist_id"], transformation_ctx="JoinAlbumArtist_node1715154429502")

# Script generated for node Join_With_Tracks
Join_With_Tracks_node1715155466206 = Join.apply(frame1=Tracks_node1715153898055, frame2=JoinAlbumArtist_node1715154429502, keys1=["id"], keys2=["track_id"], transformation_ctx="Join_With_Tracks_node1715155466206")

# Script generated for node Drop Fields
DropFields_node1715155916081 = DropFields.apply(frame=Join_With_Tracks_node1715155466206, paths=["`.id`"], transformation_ctx="DropFields_node1715155916081")

# Script generated for node Target Value
TargetValue_node1715156571219 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1715155916081, connection_type="s3", format="glueparquet", connection_options={"path": "s3://snowflakebucketsaif/csv1_output/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="TargetValue_node1715156571219")

job.commit()