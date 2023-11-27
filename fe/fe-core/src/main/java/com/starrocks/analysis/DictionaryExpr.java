// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.analysis;

import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.thrift.TDictionaryExpr;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;

import java.util.List;

public class DictionaryExpr extends FunctionCallExpr {

    private TDictionaryExpr dictionaryExpr;

    public DictionaryExpr(List<Expr> params) throws SemanticException {
        super(FunctionSet.DICTIONARY_GET, params);
    }

    public DictionaryExpr(List<Expr> params, TDictionaryExpr dictionaryExpr, Function fn) {
        super(FunctionSet.DICTIONARY_GET, params);
        this.dictionaryExpr = dictionaryExpr;
        this.fn = fn;
        setType(fn.getReturnType());
    }

    protected DictionaryExpr(DictionaryExpr other) {
        super(other);
        this.dictionaryExpr = other.getDictionaryExpr();
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.setNode_type(TExprNodeType.DICTIONARY_EXPR);
        msg.setDictionary_expr(dictionaryExpr);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDictionaryExpr(this, context);
    }

    @Override
    public boolean isAggregateFunction() {
        return false;
    }

    @Override
    public Expr clone() {
        return new DictionaryExpr(this);
    }

    public TDictionaryExpr getDictionaryExpr() {
        return dictionaryExpr;
    }

    public void setDictionaryExpr(TDictionaryExpr dictionaryExpr) {
        this.dictionaryExpr = dictionaryExpr;
    }
}
