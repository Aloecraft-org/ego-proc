import sys, json, os, time

def format_size(size_bytes):
    if size_bytes == 0: return "0B"
    # IEC format (power of 1024)
    for unit in ['B', 'KiB', 'MiB', 'GiB', 'TiB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f}{unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f}PiB"

def record():
    target = sys.argv[1]
    profile = sys.argv[2]
    stats_file = "build_stats.json"
    
    if not os.path.exists(stats_file):
        with open(stats_file, "w") as f:
            json.dump({"stats": []}, f)

    start_time = time.time()
    artifact_path = None
    success = False

    for line in sys.stdin:
        # Print the raw output so you can still see build progress/errors
        # print(line, end="")
        try:
            msg = json.loads(line)
            reason = msg.get("reason")
            
            if reason == "compiler-artifact":
                # Capture binary path
                if msg.get("target", {}).get("kind") == ["bin"]:
                    artifact_path = msg.get("filenames", [None])[0]
            
            elif reason == "build-finished":
                success = msg.get("success", False)
        except:
            continue

    duration = time.time() - start_time
    size_raw = 0
    size_human = "N/A"

    if artifact_path and os.path.exists(artifact_path):
        size_raw = os.path.getsize(artifact_path)
        size_human = format_size(size_raw)

    stat_info = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "target": target,
        "profile": profile,
        "status": "success" if success else "failed",
        "path": artifact_path or "N/A",
        "size_bytes": size_raw,
        "size_human": size_human,
        "duration_seconds": round(duration, 2)
    }

    with open(stats_file, "r+") as f:
        data = json.load(f)
        data["stats"].append(stat_info)
        f.seek(0)
        json.dump(data, f, indent=2)
        f.truncate()

if __name__ == "__main__":
    record()