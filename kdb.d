import std.stdio;
import std.string;
import std.c.stdlib;
import std.file;
import std.conv;
//import std.process;

struct Record {
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

bool contains(string[] a, string s) {
	foreach (x; a) {
		if (s == x)
			return true;
	}
	return false;
}

void find_property_value(Record[] db, string[] merge_rule, int gid, string property, string value) {
	foreach (ref x; db) {
		if (x.group == 0) {
			if (x.property == property && x.value == value) {
				set_gid_by_id(db, gid, x.id, merge_rule);
			}
		}
	}
}

void set_gid_by_id(Record[] db, int gid, int id, string[] merge_rule) {
	foreach (ref x; db) {
		if (x.id == id) {
			x.group = gid;
			if (contains(merge_rule, x.property)) {
				find_property_value(db, merge_rule, gid, x.property, x.value);
			}
		}
	}
}

void merge() {
	writeln("Merging");

	string[] merge_rule = splitLines(cast(string)read("merge_rules.txt"));

	Record[] db;

	string s;

	auto t = File("main.txt", "r");

	while (t.readln(s)) {
		string[] ss = split(chomp(s), "\t");
		Record current;
		current.id = parse!int(ss[0]);
		current.property = ss[1];
		current.value = ss[2];
		current.group = 0;
		db ~= current;
	}

	t.close;

	int gid = 0;
	int cur_id = 0;

	foreach (rec; db) {
		if (rec.group == 0) {
			gid++;
			set_gid_by_id(db, gid, rec.id, merge_rule);
		}
	}

	t = File("main.txt", "w");
	foreach (x; db) {
		t.writeln(x.id, "\t", x.property, "\t", x.value, "\t", x.group);
	}
}

void tosql() {
	auto t = File("main.txt", "r");
	auto w = File("sqlite3_query.sql", "w");
	w.writeln("CREATE TABLE main (
	cid INTEGER,
	property TEXT,
	value TEXT,
	gid INTEGER
);");

	string s;

	while (t.readln(s)) {
		string[] ss = split(chomp(s), "\t");
		w.writefln("INSERT INTO main VALUES (%s, \"%s\", \"%s\", %s);", ss[0], ss[1], ss[2], ss[3]);
	}

	w.close;

	//shell("sqlite3 main.db < sqlite3_query.sql");
	system("sqlite3 main.db < sqlite3_query.sql");
}

void sql(string[] xs) {
	string query;

	foreach (x; xs) {
		query ~= x ~ " ";
	}

	auto w = File("sqlite3_query.sql", "w");

	w.writeln(".mode tabs");
	w.writeln(query[0..$ - 1]);

	w.close;

	//writeln(shell("sqlite3 main.db < sqlite3_query.sql > sql_out.txt"));
	system("sqlite3 main.db < sqlite3_query.sql > sql_out.txt");
	//writeln(shell("sqlite3 main.db < sqlite3_query.sql"));
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

	if (args[0] == "merge")
		merge;

	if (args[0] == "tosql")
		tosql;

	if (args[0] == "sql")
		sql(args[1..$]);

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
