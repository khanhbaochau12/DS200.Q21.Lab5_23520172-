db = db.getSiblingDB("people_counter");
db.createCollection("detection_results");
db.detection_results.createIndex({ video_name: 1, frame_id: 1 });
db.detection_results.createIndex({ timestamp: -1 });
print("people_counter database initialized.");
