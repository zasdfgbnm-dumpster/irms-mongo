use irms
var fgs = [
	"ether","nitro","alchohl","nitrile","alkyne","ester","alkane","ketone",
	"amine","CCl","amide","carbonyl","anhydride","CF","aromatic","aldehyde",
	"CI","alkene","acid","CBr"
]
db.universe.createIndex(
	{ "thir.method": 1 },
	{ partialFilterExpression: { thir: { $exists: true } }, name:"thir_method"}
)
db.universe.createIndex(
	{ "expir.state": 1 },
	{ partialFilterExpression: { expir: { $exists: true } }, name:"expir_state"}
)
