#include "duckdb/parser/statement/select_statement.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

SelectStatement::SelectStatement(const SelectStatement &other) : SQLStatement(other), node(other.node->Copy()) {
	// std::cout << "Hi, I am here 1\n";
}

unique_ptr<SQLStatement> SelectStatement::Copy() const {
	// std::cout << "Hi, I am here copy\n";
	return unique_ptr<SelectStatement>(new SelectStatement(*this));
}

bool SelectStatement::Equals(const SQLStatement &other_p) const {
	if (type != other_p.type) {
		return false;
	}
	// std::cout << "Hi, I am here equals\n";
	auto &other = other_p.Cast<SelectStatement>();
	return node->Equals(other.node.get());
}

string SelectStatement::ToString() const {
	// std::cout << "Hi, I am here tostring\n";
	return node->ToString();
}

} // namespace duckdb
