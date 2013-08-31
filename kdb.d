import std.stdio;
import std.string;
import std.c.stdlib;
import std.file;
import std.conv;
import std.array;
import etc.c.sqlite3;

extern (C) int callback(void* result, int cnt, char** values, char** columns) {
	foreach (i; 0..cnt) {
		*cast(string*)result ~= to!string(values[i]);

		if (i != cnt - 1) {
			*cast(string*)result ~= "\t";
		} else {
			*cast(string*)result ~= "\r\n";
		}
	}

	return 0;
}

string sql_exec(string cmd) {
	string result;
	char* err;

	sqlite3* db;
	sqlite3_open("main.db", &db);

	auto rc = sqlite3_exec(db, toStringz(cmd), &callback, &result, &err);

	if (rc)
		writeln("SQL Error: ", to!string(err));

	sqlite3_close(db);

	return result;
}

string glue_string(string[] xs) {
	string ys;
	foreach (x; xs)
		ys ~= x ~ " ";
	return ys[0..$ - 1];
}

int read_cnt() {
	return to!int(readText("cnt.txt"));
}

void store_cnt(int cnt) {
	std.file.write("cnt.txt", to!string(cnt));
}

void import_db(string path) {
	auto t = File(path, "r");
	string s;

	int cnt = read_cnt;

	t.readln(s);

	string[] properties = split(chomp(s), "\t");

	while (t.readln(s)) {
		string[] values = split(chomp(s), "\t");

		foreach (i; 0..properties.length) {
			if (!values[i].empty)
				sql_exec("INSERT INTO main VALUES (" ~ to!string(cnt) ~ ", \"" ~ properties[i] ~ "\", \"" ~ values[i] ~ "\", 0);");
		}

		cnt++;
	}

	store_cnt(cnt);
}

void export_db() {
	auto t = File("sql_out.txt", "r");
	auto w = File("export_out.txt", "w");
	string s;

	int[string][string][string] a;
	int[string] b;

	while (t.readln(s)) {
		string[] ss = split(chomp(s), "\t");
		a[ss[3]][ss[1]][ss[2]]++;
	}

	a.rehash;

	foreach (x; a) {
		foreach (z, y; x) {
			b[z] = 0;
		}
	}

	foreach (x; a) {
		foreach (z, y; x) {
			if (y.length > b[z])
				b[z] = y.length;
		}
	}

	w.write("gid");
	foreach (y, x; b) {
		foreach (i; 0..x) {
			w.write("\t", y);
		}
	}
	w.writeln;

	foreach (y, ref x; a) {
		w.write(y);
		foreach (yb, ref xb; b) {
			foreach (i; 0..xb) {
				w.write("\t");
				if (yb in x) {
					if (x[yb].length > 0) {
						w.write(x[yb].keys[0]);
						x[yb].remove(x[yb].keys[0]);
					}
				}
			}
		}
		w.writeln;
	}
}

int merging_possible(string gid, string[] merge_rules) {
	foreach (x; merge_rules) {
		string result = sql_exec("SELECT cid FROM main WHERE gid = 0 AND cid IN (
			SELECT cid FROM main WHERE property = \"" ~ x ~ "\" AND value IN (
			SELECT value FROM main WHERE property = \"" ~ x ~ "\" AND gid = " ~ gid ~ "));");

		if (!result.empty)
			return 1;
	}

	return 0;
}

void set_gid_by_id(string gid, string cid, string[] merge_rules) {
	sql_exec("UPDATE main SET gid = " ~ gid ~ " WHERE cid = " ~ cid ~ ";");

	while (merging_possible(gid, merge_rules)) {
		foreach (x; merge_rules) {
			sql_exec("UPDATE main SET gid = " ~ gid ~ " WHERE cid IN (
				SELECT cid FROM main WHERE gid = 0 AND cid IN (
				SELECT cid FROM main WHERE property = \"" ~ x ~ "\" AND value IN (
				SELECT value FROM main WHERE property = \"" ~ x ~ "\" AND gid = " ~ gid ~ ")));");
		}
	}
}

int get_next_record(ref string s) {
	s = sql_exec("SELECT cid FROM main WHERE gid = 0 LIMIT 1;");

	return s.empty ? 0 : 1;
}

void merge() {
	writeln("Merging...");
	sql_exec("UPDATE main SET gid = 0;");

	string[] merge_rules = splitLines(readText("merge_rules.txt"));

	string cid;
	int gid = 1;

	while (get_next_record(cid)) {
		set_gid_by_id(to!string(gid), cid, merge_rules);
		gid++;
	}
}

void sql(string query) {
	string result = sql_exec(query);

	writeln(result);
	std.file.write("sql_out.txt", result);
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

	if (args[0] == "sql")
		sql(glue_string(args[1..$]));

	if (args[0] == "export")
		export_db;
}

void main(string[] args) {
	string query;

	if (args.length > 1) {
		query = glue_string(args[1..$]);

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
