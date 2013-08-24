import std.stdio;
import std.string;

struct record {
	int id;
	string property;
	string value;
	int group;
}

int read_cnt() {
	auto t = File("cnt.txt", "r");
	int cnt;
	t.readf("%s", &cnt);
	return cnt;
}

void store_cnt(int cnt) {
	auto t = File("cnt.txt", "w");
	t.writeln(cnt);
}

void import_db(string path) {
	auto t = File(path, "r");
	auto w = File("main.txt", "a");
	string s;
	string[] properties, values;

	int cnt = read_cnt;

	t.readln(s);

	properties = split(chomp(s), "\t");

	while (t.readln(s)) {
		values = split(chomp(s), "\t");

		foreach (i, x; properties) {
			if (values[i] != "")
				w.writeln(cnt, "\t", x, "\t", values[i]);
		}

		cnt++;
	}

	store_cnt(cnt);
}

void read_query(string path) {
	auto t = File(path, "r");
}

void parse_query(string query) {
	string[] args = split(query);

	if (args[0] == "exec")
		read_query(args[1]);

	if (args[0] == "import")
		import_db(args[1]);

	//if (args[0] == "merge")

	//switch (args[0]) {
	//	case "import" : writeln(args[1]); break;
	//	default : writeln("no such command");
	//}
}

void main() {
	string query;

	while (query != "exit") {
		if (query != "")
			parse_query(query);

		stdin.readln(query);
		query = chomp(query);
	}
}