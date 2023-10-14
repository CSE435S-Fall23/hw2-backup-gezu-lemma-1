package hw1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.util.TablesNamesFinder;

public class Query {

	private String q;

	public Query(String q) {
		this.q = q;
	}

	public Relation execute() {
		Statement statement = null;
		try {
			statement = CCJSqlParserUtil.parse(q);
		} catch (JSQLParserException e) {
			System.out.println("Unable to parse query");
			e.printStackTrace();
		}
		Select selectStatement = (Select) statement;
		PlainSelect sb = (PlainSelect) selectStatement.getSelectBody();

		// your code here

		// Get the table and its original tuples, td, relation...etc
		Catalog catalog = Database.getCatalog();
		Table fromTable = (Table) sb.getFromItem();
		int tableId = catalog.getTableId(fromTable.getFullyQualifiedName());
		ArrayList<Tuple> originalTuples = catalog.getDbFile(tableId).getAllTuples();
		TupleDesc originalTd = catalog.getDbFile(tableId).getTupleDesc();
		Relation relation = new Relation(originalTuples, originalTd);

		// JOIN
		if (sb.getJoins() != null) {
			relation = handleJoin(catalog, sb, relation);
		}

		// SELECT
		relation = handleSelect(sb, relation);

		// WHERE
		if (sb.getWhere() != null) {
			relation = handleWhere(sb, relation);
		}

		// TODO: Implement AS

		return relation;

	}

	private Relation handleWhere(PlainSelect sb, Relation relation) {
		// TODO: Dealing with multiple WHEREs ?
		String whereClause = sb.getWhere().toString();

		// Parse WHERE clause into field and value using regex
		String wherePattern = "([a-zA-Z\\d]+)\\s=\\s([a-zA-Z\\d]+)";
		Pattern pat = Pattern.compile(wherePattern);
		Matcher mat = pat.matcher(whereClause);
		mat.matches();

		int whereFieldIndex = relation.getDesc().nameToId(mat.group(1));

		Field whereField = null;
		if (relation.getDesc().getType(whereFieldIndex).equals(Type.INT)) {
			whereField = new IntField(Integer.parseInt(mat.group(2)));
		} else if (relation.getDesc().getType(whereFieldIndex).equals(Type.STRING)) {
			whereField = new StringField(mat.group(2));
		}

		return relation.select(whereFieldIndex, RelationalOperator.EQ, whereField);
	}

	private Relation handleJoin(Catalog catalog, PlainSelect sb, Relation relation) {
		List<Join> joins = sb.getJoins();
		for (Join join : joins) {

			// Get information for the new table
			String rightTableName = join.getRightItem().toString();
			int rightTableId = catalog.getTableId(rightTableName);
			ArrayList<Tuple> rightTuples = catalog.getDbFile(rightTableId).getAllTuples();
			TupleDesc rightTd = catalog.getDbFile(rightTableId).getTupleDesc();
			Relation rightTableRelation = new Relation(rightTuples, rightTd);

			// Parse JOIN fields using regex
			String joinClause = join.getOnExpression().toString();
			String joinPattern = "([a-zA-Z\\d]+)\\.([a-zA-Z\\d]+)\\s=\\s([a-zA-Z\\d]+)\\.([a-zA-Z\\d]+)";
			Pattern pat = Pattern.compile(joinPattern);
			Matcher mat = pat.matcher(joinClause);
			mat.matches();

			// Group 2 holds the field for the other relation. Group 4 holds the field for
			// this relation
			int originalFieldIndex = relation.getDesc().nameToId(mat.group(2));
			int rightFieldIndex = rightTd.nameToId(mat.group(4));

			relation = relation.join(rightTableRelation, originalFieldIndex, rightFieldIndex);
		}
		return relation;
	}

	private Relation handleSelect(PlainSelect sb, Relation relation) {
		// SELECT
		List<SelectItem> selectItems = sb.getSelectItems();
		ArrayList<Integer> SelectItemsFieldNums = new ArrayList<Integer>();
		Boolean isGroupBy = !(sb.getGroupByColumnReferences() == null);

		if (!selectItems.isEmpty() && !selectItems.get(0).toString().equals("*")) {
			for (SelectItem selectItem : selectItems) {

				Expression expression = ((SelectExpressionItem) selectItem).getExpression();
				// AGGREGATE
				if (expression instanceof Function) {
					Function function = (Function) expression;
					String functionName = function.getName();
					String originalFieldName = function.getParameters().getExpressions().get(0).toString();

					/*
					 * Because we were instructed to design aggregate to always expect a single
					 * column for !groupBy, we need to get rid of the other fields first in that
					 * case
					 */
					if (!isGroupBy) {
						ArrayList<Integer> onlyAggregateField = new ArrayList<>();
						onlyAggregateField.add(relation.getDesc().nameToId(originalFieldName));
						relation = relation.project(onlyAggregateField);
					}

					switch (functionName) {
					case "SUM":
						relation = relation.aggregate(AggregateOperator.SUM, isGroupBy);
						break;
					case "MAX":
						relation = relation.aggregate(AggregateOperator.MAX, isGroupBy);
						break;
					case "MIN":
						relation = relation.aggregate(AggregateOperator.MIN, isGroupBy);
						break;
					case "AVG":
						relation = relation.aggregate(AggregateOperator.AVG, isGroupBy);
						break;
					case "COUNT":
						relation = relation.aggregate(AggregateOperator.COUNT, isGroupBy);
						break;
					}

					SelectItemsFieldNums.add(relation.getDesc().nameToId(originalFieldName));
				} else {
					SelectItemsFieldNums.add(relation.getDesc().nameToId(selectItem.toString()));
				}
			}
			return relation.project(SelectItemsFieldNums);
		}
		// Just return original if universal operator * is specified
		return relation;

	}

}
