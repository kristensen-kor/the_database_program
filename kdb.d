import std.stdio;
import std.string;
import std.c.stdlib;
import std.file;

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
	writeln("Reading query from ", path);

	if (!exists(path)) {
		writeln("Error! ", path, "not found.");
		return;
	}

	auto t = File(path, "r");

	string query;

	while (t.readln(query)) {
		query = chomp(query);
		writeln(path, ">", query);

		if (query != "")
			parse_query(query);
	}
}

void parse_query(string query) {
	string[] args = split(query);

	if (args[0] == "exit")
		exit(0);

	if (args[0] == "exec")
		read_query(args[1]);

	if (args[0] == "import")
		import_db(args[1]);

	//if (args[0] == "merge")

}

void main(string[] args) {
	string query;

	if (args.length > 1) {
		foreach (i, x; args) {
			if (i != 0)
				query ~= x ~ " ";
		}
		writeln("args>", query);
		parse_query(query);
	} else {
		write(">");

		while (stdin.readln(query)) {
			query = chomp(query);

			if (query != "")
				parse_query(query);

			write(">");
		}
	}
}