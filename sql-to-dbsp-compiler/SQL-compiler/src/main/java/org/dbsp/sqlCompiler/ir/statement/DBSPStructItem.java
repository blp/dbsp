package org.dbsp.sqlCompiler.ir.statement;

import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.IHasType;
import org.dbsp.util.IIndentStream;

/** An item that declares a struct. */
@NonCoreIR
public final class DBSPStructItem extends DBSPItem implements IHasType {
    public final DBSPTypeStruct type;

    public DBSPStructItem(DBSPTypeStruct type) {
        this.type = type;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        this.type.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public DBSPType getType() {
        return this.type;
    }

    @Override
    public boolean sameFields(IDBSPNode other) {
        DBSPStructItem o = other.as(DBSPStructItem.class);
        if (o == null)
            return false;
        return this.type.sameType(o.type);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.type);
    }

    @Override
    public DBSPStatement deepCopy() {
        return new DBSPStructItem(this.type);
    }

    @Override
    public EquivalenceResult equivalent(EquivalenceContext context, DBSPStatement other) {
        // Since this is NonCoreIR we leave this for later
        return new EquivalenceResult(false, context);
    }
}
