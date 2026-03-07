import urllib.request
import json

def discover_schema():
    print("🔍 正在从服务器提取【Item】对象的所有可用字段字典...")
    
    # GraphQL 内省查询：查询 Item 类型下所有的字段名和描述
    query = """
    {
      __type(name: "Item") {
        fields {
          name
          description
          type {
            name
            kind
            ofType {
              name
              kind
            }
          }
        }
      }
    }
    """
    
    url = 'https://api.tarkov.dev/graphql'
    headers = {
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    req = urllib.request.Request(url, data=json.dumps({'query': query}).encode('utf-8'), headers=headers)
    
    try:
        with urllib.request.urlopen(req) as response:
            res_data = json.loads(response.read().decode('utf-8'))
            fields = res_data['data']['__type']['fields']
            
            # 格式化输出
            print(f"\n{'字段名称':<25} | {'字段描述'}")
            print("-" * 80)
            
            inventory = []
            for f in fields:
                name = f['name']
                desc = f['description'] or "无描述"
                print(f"{name:<25} | {desc}")
                inventory.append({"field": name, "description": desc})
            
            # 保存为本地参考字典
            with open("src/test/api_field_dictionary.json", "w", encoding="utf-8") as f:
                json.dump(inventory, f, indent=4, ensure_ascii=False)
                
            print(f"\n✅ 探测完毕！共发现 {len(fields)} 个可用字段。")
            print("📑 完整字典已保存至: src/test/api_field_dictionary.json")

    except Exception as e:
        print(f"❌ 探测失败: {e}")

if __name__ == "__main__":
    discover_schema()