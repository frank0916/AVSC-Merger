package loadtester;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;

public class AvscPaser {
	final static String NAME = "name";
	final static String NAME_SPACE = "namespace";

	private Pattern pattern = Pattern
			.compile("\"([a-zA-Z][a-zA-Z0-9_]*\\.)*[a-zA-Z][a-zA-Z0-9_]*\"");

	private Map<String, String> unsolved = new HashMap<>();

	private Parser parser;
	private Map<String, Schema> schemas = null;

	public Parser getParser() {
		return parser;
	}

	public void register(File file) {
		try {
			String s = Files.lines(Paths.get(file.getPath()))
					.collect(Collectors.joining());
			int start = s.indexOf("{") + 1;
			int end = s.indexOf(",", start);
			String name = s.substring(start, end);
			name = name.split(":")[1].replace("\"", "").trim();
			unsolved.put(name, s);
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}

	public void solve(String key) {
		if (schemas.containsKey(key)) {
			return;
		}

		String text = unsolved.get(key);
		if (text == null) {
			return;
		}

		String prefix = key.substring(0, key.lastIndexOf('.') + 1);
		Matcher m = pattern.matcher(text);

		while (m.find()) {
			String token = m.group();
			String name = token.substring(1, token.length() - 1);
			if (name.contains(".")) {
				solve(name);
			} else {
				String fullName = prefix + name;
				if (!key.equals(fullName)) {
					solve(fullName);
				}
			}
		}

		Schema schema = parser.parse(text);
		schemas.put(key, schema);
	}

	String getValue(String key, String text) {
		int start = text.indexOf(key) + key.length() + 2;
		start = text.indexOf('"', start) + 1;
		int end = text.indexOf('"', start);
		return text.substring(start, end);
	}

	void register(String text) {
		String name = getValue(NAME, text);
		String namespace = getValue(NAME_SPACE, text);
		String key = namespace + "." + name;
		unsolved.put(key, text);
	}

	Map<String, Schema> registerRoot(String avscRoot, int depth) {
		try (Stream<Path> stream = Files.walk(Paths.get(avscRoot), depth)) {
			stream.filter(p -> p.toFile().isFile())
					.filter(p -> p.toString().endsWith(".avsc"))
					.map(p -> Utils.read(p)).forEach(t -> register(t));
			schemas = new HashMap<>();
			Set<String> keys = unsolved.keySet();
			parser = new Schema.Parser();
			for (String key : keys) {
				solve(key);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return schemas;
	}
}