CREATE (pam:Person {name: "Pam"}), (tom:Person {name: "Tom"}), (kate:Person {name: "Kate"}), (mary:Person {name: "Mary"}), (bob:Person {name: "Bob"}), (liz:Person {name: "Liz"}), (dick:Person {name: "Dick"}), (ann:Person {name: "Ann"}), (pat:Person {name: "Pat"}), (jack:Person {name: "Jack"}), (jim:Person {name: "Jim"}), (joli:Person {name: "Joli"}), (pam)-[:PARENT]->(bob), (tom)-[:PARENT]->(bob), (tom)-[:PARENT]->(liz), (kate)-[:PARENT]->(liz), (mary)-[:PARENT]->(ann), (bob)-[:PARENT]->(ann), (bob)-[:PARENT]->(pat), (dick)-[:PARENT]->(jim), (ann)-[:PARENT]->(jim), (pat)-[:PARENT]->(joli), (jack)-[:PARENT]->(joli)


MATCH (a:Person {name: "Pam"}), (b:Person {name: "Bob"}), (a)-[:PARENT]->(b) return a, b

MATCH (a), (b), (a)-[:PARENT]->(b) return a, b

MATCH (a), (b), (a)-->(b) return a, b


MATCH (a:Person {name: "Bob"}), (child:Person), (a)-[:PARENT]->(child) RETURN child.name

MATCH (parent: Person), (a: Person), (parent)-[:PARENT]->(a) RETURN parent.name


MATCH (p:Person) WHERE p.name = "Pam" SET p:Female

MATCH (p:Person) WHERE p:Female RETURN p.name


MATCH (brother:Person:Male)<-[:PARENT]-()-[:PARENT]->(p:Person)
RETURN brother.name, p.name

MATCH (brother:Person:Male), (a), (brother)<-[:PARENT]-(a), (a)-[:PARENT]->(p:Person)
RETURN brother.name, p.name
